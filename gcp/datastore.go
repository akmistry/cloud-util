package gcp

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/datastore"
	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/iterator"

	"github.com/akmistry/cloud-util"
)

const (
	datastoreRetryInterval = time.Second

	maxTries = 5
)

type Datastore struct {
	cloud.BaseStore

	name       string
	entityName string
	client     *datastore.Client

	requestsCounter *prom.CounterVec
}

type Entity struct {
	Value []byte `datastore:",noindex"`
}

func NewDatastore(name string) (*Datastore, error) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, *Project)
	if err != nil {
		return nil, err
	}

	rc := prom.NewCounterVec(prom.CounterOpts{
		Name:        "gcp_datastore_requests_total",
		Help:        "Number of GCP datastore requests",
		ConstLabels: prom.Labels{"name": name},
	}, []string{"method"})
	prom.MustRegister(rc)

	return &Datastore{
		name:            name,
		entityName:      "Entity-" + name,
		client:          client,
		requestsCounter: rc,
	}, nil
}

func (s *Datastore) createKey(k string) *datastore.Key {
	return datastore.NameKey(s.entityName, k, nil)
}

func (s *Datastore) Get(key string) (*cloud.KVPair, error) {
	s.requestsCounter.WithLabelValues("get").Inc()
	var entity Entity
	var lastErr error
	for i := 0; i < maxTries; i++ {
		err := s.client.Get(context.TODO(), s.createKey(key), &entity)
		if err == datastore.ErrNoSuchEntity {
			return nil, cloud.ErrKeyNotFound
		} else if err == nil {
			return &cloud.KVPair{Key: key, Value: entity.Value}, nil
		}
		lastErr = err
		time.Sleep(datastoreRetryInterval)
	}
	return nil, fmt.Errorf("Datastore.Get exceeded max tries, lastErr: %v", lastErr)
}

func (s *Datastore) Put(key string, value []byte, options *cloud.WriteOptions) error {
	s.requestsCounter.WithLabelValues("put").Inc()
	entity := Entity{Value: value}
	dsKey := s.createKey(key)
	var lastErr error
	for i := 0; i < maxTries; i++ {
		_, err := s.client.Put(context.TODO(), dsKey, &entity)
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(datastoreRetryInterval)
	}
	return fmt.Errorf("Datastore.Put exceeded max tries, lastErr: %v", lastErr)
}

func (s *Datastore) Delete(key string) error {
	s.requestsCounter.WithLabelValues("delete").Inc()
	var lastErr error
	dsKey := s.createKey(key)
	for i := 0; i < maxTries; i++ {
		err := s.client.Delete(context.TODO(), dsKey)
		if err == nil || err == datastore.ErrNoSuchEntity {
			return nil
		}
		lastErr = err
		time.Sleep(datastoreRetryInterval)
	}
	return fmt.Errorf("Datastore.Delete exceeded max tries, lastErr: %v", lastErr)
}

func (s *Datastore) Exists(key string) (bool, error) {
	_, err := s.Get(key)
	if err == cloud.ErrKeyNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Datastore) AtomicPut(key string, value []byte, previous *cloud.KVPair, options *cloud.WriteOptions) (bool, *cloud.KVPair, error) {
	s.requestsCounter.WithLabelValues("put_txn").Inc()

	entity := Entity{Value: value}
	dsKey := s.createKey(key)
	_, err := s.client.RunInTransaction(context.TODO(), func(tx *datastore.Transaction) error {
		var oldEntity Entity
		err := tx.Get(dsKey, &oldEntity)
		if err == datastore.ErrNoSuchEntity {
			if previous != nil {
				return cloud.ErrKeyNotFound
			}
		} else if err != nil {
			return err
		} else if previous == nil {
			return cloud.ErrKeyExists
		}

		if previous != nil && !bytes.Equal(previous.Value, oldEntity.Value) {
			return cloud.ErrKeyModified
		}

		_, err = tx.Put(dsKey, &entity)
		return err
	})

	if err != nil {
		return false, nil, err
	}

	updated := &cloud.KVPair{
		Key:   key,
		Value: value,
	}
	return true, updated, nil
}

func (s *Datastore) AtomicDelete(key string, previous *cloud.KVPair) (bool, error) {
	s.requestsCounter.WithLabelValues("delete_txn").Inc()

	if previous == nil {
		return false, cloud.ErrPreviousNotSpecified
	}

	dsKey := s.createKey(key)
	_, err := s.client.RunInTransaction(context.TODO(), func(tx *datastore.Transaction) error {
		var oldEntity Entity
		err := tx.Get(dsKey, &oldEntity)
		if err == datastore.ErrNoSuchEntity {
			return cloud.ErrKeyNotFound
		} else if err != nil {
			return err
		}

		if !bytes.Equal(previous.Value, oldEntity.Value) {
			return cloud.ErrKeyModified
		}

		return tx.Delete(dsKey)
	})

	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Datastore) ListEx(start string, cursor []byte) (keys []string, nextCursor []byte, err error) {
	s.requestsCounter.WithLabelValues("list").Inc()
	if len(start) > 0 && len(cursor) > 0 {
		panic("start and cursor cannot both be set")
	}
	q := datastore.NewQuery(s.entityName).KeysOnly().Limit(1024)
	if len(cursor) > 0 {
		c, err := datastore.DecodeCursor(string(cursor))
		if err != nil {
			return nil, nil, fmt.Errorf("Datastore.List error decoding cursor: %v", err)
		}
		q = q.Start(c)
	} else if len(start) > 0 {
		q = q.Filter("__key__ >=", s.createKey(start))
	}

	iter := s.client.Run(context.TODO(), q)
	for {
		k, err := iter.Next(nil)
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, nil, fmt.Errorf("Datastore.List error getting next key: %v", err)
		}
		keys = append(keys, k.Name)
	}
	if len(keys) > 0 {
		c, err := iter.Cursor()
		if err != nil {
			return nil, nil, fmt.Errorf("Datastore.List error getting cursor: %v", err)
		}
		nextCursor = []byte(c.String())
	}
	return keys, nextCursor, nil
}
