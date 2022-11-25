package rpc

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/akmistry/cloud-util"
	"github.com/akmistry/cloud-util/rpc/pb"
)

type Store struct {
	name   string
	client pb.StoreClient
}

var _ = (cloud.UnorderedStore)((*Store)(nil))

func NewStore(addr, name string) (*Store, error) {
	conn, err := grpc.Dial(addr, grpc.WithBlock(), grpc.WithInsecure(), grpc.WithTimeout(5*time.Second))
	if err != nil {
		return nil, err
	}

	return &Store{name: name, client: pb.NewStoreClient(conn)}, nil
}

func (s *Store) GetContext(ctx context.Context, key string) (*cloud.KVPair, error) {
	req := &pb.GetRequest{
		DbName: s.name,
		Key:    key,
	}
	resp, err := s.client.Get(ctx, req)
	if err != nil {
		switch status.Code(err) {
		case codes.NotFound:
			return nil, cloud.ErrKeyNotFound
		}
		return nil, err
	} else if resp.Val == nil {
		return nil, cloud.ErrKeyNotFound
	}
	return &cloud.KVPair{Key: key, Value: resp.Val}, nil
}

func (s *Store) Get(key string) (*cloud.KVPair, error) {
	return s.GetContext(context.Background(), key)
}

func (s *Store) Exists(key string) (bool, error) {
	// TODO: Turn this into a RPC method to avoid transferring the value.
	_, err := s.Get(key)
	if err == cloud.ErrKeyNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) Put(key string, value []byte, options *cloud.WriteOptions) error {
	req := &pb.PutRequest{
		DbName: s.name,
		Key:    key,
		Val:    value,
	}
	_, err := s.client.Put(context.TODO(), req)
	return err
}

func (s *Store) Delete(key string) error {
	req := &pb.DeleteRequest{
		DbName: s.name,
		Key:    key,
	}
	_, err := s.client.Delete(context.TODO(), req)
	return err
}

// TODO: Implement.
func (s *Store) AtomicPut(key string, value []byte, previous *cloud.KVPair, options *cloud.WriteOptions) (bool, *cloud.KVPair, error) {
	req := &pb.AtomicPutRequest{
		DbName: s.name,
		Key:    key,
		Val:    value,
	}
	if previous != nil {
		req.OldVal = previous.Value
	}
	_, err := s.client.AtomicPut(context.TODO(), req)
	switch status.Code(err) {
	case codes.OK:
		updated := &cloud.KVPair{
			Key:   key,
			Value: value,
		}
		return true, updated, nil
	case codes.NotFound:
		return false, nil, cloud.ErrKeyNotFound
	case codes.AlreadyExists:
		return false, nil, cloud.ErrKeyExists
	case codes.Aborted:
		return false, nil, cloud.ErrKeyModified
	default:
		return false, nil, err
	}
}

func (s *Store) AtomicDelete(key string, previous *cloud.KVPair) (bool, error) {
	if previous == nil {
		// Not specifying a previous is a programming error.
		panic("previous == nil")
	}

	req := &pb.AtomicDeleteRequest{
		DbName: s.name,
		Key:    key,
		OldVal: previous.Value,
	}
	_, err := s.client.AtomicDelete(context.TODO(), req)
	switch status.Code(err) {
	case codes.OK:
		return true, nil
	case codes.NotFound:
		return false, cloud.ErrKeyNotFound
	case codes.Aborted:
		return false, cloud.ErrKeyModified
	default:
		return false, err
	}
}

func (s *Store) ListEx(start string, cursor []byte) (keys []string, nextCursor []byte, err error) {
	if len(start) > 0 && len(cursor) > 0 {
		panic("start and cursor cannot both be set")
	}
	req := &pb.ListRequest{
		DbName:   s.name,
		StartKey: start,
		Cursor:   cursor,
	}
	resp, err := s.client.List(context.TODO(), req)
	if err != nil {
		return nil, nil, err
	}
	if len(resp.Keys) > 0 {
		keys := make([]string, 0, len(resp.Keys))
		for _, k := range resp.Keys {
			keys = append(keys, k)
		}
	}
	return keys, resp.Cursor, nil
}
