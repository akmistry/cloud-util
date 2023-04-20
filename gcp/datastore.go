package gcp

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"time"

	"cloud.google.com/go/datastore"
	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/iterator"

	"github.com/akmistry/cloud-util"
)

const (
	scheme = "gcp"

	datastoreTimeout = time.Second
	maxTries         = 5
)

type Datastore struct {
	name       string
	entityKind string
	client     *datastore.Client

	requestsCounter *prom.CounterVec
}

var _ = (cloud.OrderedStore)((*Datastore)(nil))
var _ = (cloud.AtomicUnorderedStore)((*Datastore)(nil))

func init() {
	cloud.RegisterStoreScheme(scheme, openStore)
}

// URL format: gcp://datastore/<table name>
func openStore(path string) (cloud.UnorderedStore, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	if u.Scheme != scheme {
		return nil, fmt.Errorf("cloud/gcp: unexpected scheme: %s", u.Scheme)
	} else if u.Host == "" {
		return nil, fmt.Errorf("cloud/gcp: empty type")
	} else if u.Path[0] != '/' {
		return nil, fmt.Errorf("cloud/gcp: invalid path: %s", u.Path)
	}
	name := u.Path[1:]
	if name == "" {
		return nil, fmt.Errorf("cloud/gcp: empty table name")
	}

	// TODO: Support Bigtable
	switch u.Host {
	case "datastore":
		return NewDatastore(name)
	default:
		return nil, fmt.Errorf("cloud/gcp: invalid type: %s", u.Host)
	}
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
		entityKind:      "Entity-" + name,
		client:          client,
		requestsCounter: rc,
	}, nil
}

func (s *Datastore) createKey(k string) *datastore.Key {
	return datastore.NameKey(s.entityKind, k, nil)
}

func (s *Datastore) Get(key string) (*cloud.KVPair, error) {
	s.requestsCounter.WithLabelValues("get").Inc()
	var entity Entity
	var lastErr error
	for i := 0; i < maxTries; i++ {
		ctx, cf := context.WithTimeout(context.Background(), datastoreTimeout)
		err := s.client.Get(ctx, s.createKey(key), &entity)
		cf()
		if err == datastore.ErrNoSuchEntity {
			return nil, cloud.ErrKeyNotFound
		} else if err == nil {
			return &cloud.KVPair{Key: key, Value: entity.Value}, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("Datastore.Get exceeded max tries, lastErr: %v", lastErr)
}

func (s *Datastore) Put(key string, value []byte, options *cloud.WriteOptions) error {
	s.requestsCounter.WithLabelValues("put").Inc()
	entity := Entity{Value: value}
	dsKey := s.createKey(key)
	var lastErr error
	for i := 0; i < maxTries; i++ {
		ctx, cf := context.WithTimeout(context.Background(), datastoreTimeout)
		_, err := s.client.Put(ctx, dsKey, &entity)
		cf()
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("Datastore.Put exceeded max tries, lastErr: %v", lastErr)
}

func (s *Datastore) Delete(key string) error {
	s.requestsCounter.WithLabelValues("delete").Inc()
	var lastErr error
	dsKey := s.createKey(key)
	for i := 0; i < maxTries; i++ {
		ctx, cf := context.WithTimeout(context.Background(), datastoreTimeout)
		err := s.client.Delete(ctx, dsKey)
		cf()
		if err == nil || err == datastore.ErrNoSuchEntity {
			return nil
		}
		lastErr = err
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
	ctx, cf := context.WithTimeout(context.Background(), datastoreTimeout)
	defer cf()
	_, err := s.client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
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
	ctx, cf := context.WithTimeout(context.Background(), datastoreTimeout)
	defer cf()
	_, err := s.client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
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

func (s *Datastore) ListKeys(start string) ([]string, error) {
	s.requestsCounter.WithLabelValues("list").Inc()
	q := datastore.NewQuery(s.entityKind).KeysOnly().Limit(1024)
	if len(start) > 0 {
		q = q.FilterField("__key__", ">=", s.createKey(start))
	}

	var keys []string
	ctx, cf := context.WithTimeout(context.Background(), datastoreTimeout)
	defer cf()
	iter := s.client.Run(ctx, q)
	for {
		k, err := iter.Next(nil)
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, fmt.Errorf("Datastore.List error getting next key: %v", err)
		}
		keys = append(keys, k.Name)
	}
	return keys, nil
}
