package rpc

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/akmistry/cloud-util"
	"github.com/akmistry/cloud-util/rpc/pb"
)

const (
	scheme = "rpc"
)

type Store struct {
	name   string
	client pb.StoreClient
}

var _ = (cloud.UnorderedStore)((*Store)(nil))

func init() {
	cloud.RegisterStoreScheme(scheme, openStore)
}

func openStore(path string) (cloud.UnorderedStore, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	if u.Scheme != scheme {
		return nil, fmt.Errorf("cloud/rpc: unexpected scheme: %s", u.Scheme)
	} else if u.Host == "" {
		return nil, fmt.Errorf("cloud/rpc: empty host")
	} else if u.Path[0] != '/' {
		return nil, fmt.Errorf("cloud/rpc: invalid path: %s", u.Path)
	}
	name := u.Path[1:]
	if name == "" {
		return nil, fmt.Errorf("cloud/rpc: empty name")
	}
	return NewStore(u.Host, name)
}

func NewStore(addr, name string) (*Store, error) {
	conn, err := grpc.Dial(addr, grpc.WithBlock(), grpc.WithInsecure(), grpc.WithTimeout(5*time.Second))
	if err != nil {
		return nil, err
	}

	return &Store{name: name, client: pb.NewStoreClient(conn)}, nil
}

func translateError(err error) error {
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.NotFound:
		return cloud.ErrKeyNotFound
	case codes.AlreadyExists:
		return cloud.ErrKeyExists
	case codes.Aborted:
		return cloud.ErrKeyModified
	case codes.Unimplemented:
		return cloud.ErrCallNotSupported
	default:
		return err
	}
}

func (s *Store) GetContext(ctx context.Context, key string) (*cloud.KVPair, error) {
	req := &pb.GetRequest{
		DbName: s.name,
		Key:    key,
	}
	resp, err := s.client.Get(ctx, req)
	if err != nil {
		return nil, translateError(err)
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
	return translateError(err)
}

func (s *Store) Delete(key string) error {
	req := &pb.DeleteRequest{
		DbName: s.name,
		Key:    key,
	}
	_, err := s.client.Delete(context.TODO(), req)
	return translateError(err)
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
	if err != nil {
		return false, nil, translateError(err)
	}
	updated := &cloud.KVPair{
		Key:   key,
		Value: value,
	}
	return true, updated, nil
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
	if err != nil {
		return false, translateError(err)
	}
	return true, nil
}

func (s *Store) ListKeys(start string) (keys []string, err error) {
	req := &pb.ListRequest{
		DbName:   s.name,
		StartKey: start,
	}
	resp, err := s.client.List(context.TODO(), req)
	if err != nil {
		return nil, translateError(err)
	}
	return resp.Keys, nil
}
