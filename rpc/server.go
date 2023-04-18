package rpc

import (
	"context"
	"errors"
	"log"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/akmistry/cloud-util"
	"github.com/akmistry/cloud-util/rpc/pb"
)

type OpenStoreFunc func(string) (cloud.UnorderedStore, error)

type Server struct {
	pb.UnimplementedStoreServer
	f OpenStoreFunc

	stores map[string]*storeEntry
	lock   sync.Mutex
}

type storeEntry struct {
	store cloud.UnorderedStore
}

func NewServer(f OpenStoreFunc) *Server {
	return &Server{
		f:      f,
		stores: make(map[string]*storeEntry),
	}
}

func (s *Server) Shutdown() {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, store := range s.stores {
		cloud.DoStoreClose(store.store)
	}
	s.stores = nil
}

func (s *Server) getStore(name string) (*storeEntry, error) {
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "Empty DB name")
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	e := s.stores[name]
	if e == nil {
		store, err := s.f(name)
		if err != nil {
			return nil, err
		}

		e = &storeEntry{
			store: store,
		}
		s.stores[name] = e
	}
	return e, nil
}

func makeGrpcError(err error) error {
	if errors.Is(err, cloud.ErrKeyNotFound) {
		return status.Error(codes.NotFound, err.Error())
	} else if errors.Is(err, cloud.ErrKeyExists) {
		return status.Error(codes.AlreadyExists, err.Error())
	} else if errors.Is(err, cloud.ErrKeyModified) {
		return status.Error(codes.Aborted, err.Error())
	}
	return err
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	store, err := s.getStore(req.DbName)
	if err != nil {
		return nil, err
	}

	log.Printf("Fetching key: %s", req.Key)
	item, err := store.store.Get(req.Key)
	if err != nil {
		return nil, makeGrpcError(err)
	}

	return &pb.GetResponse{Val: item.Value}, nil
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	store, err := s.getStore(req.DbName)
	if err != nil {
		return nil, err
	}

	err = store.store.Put(req.Key, req.Val, nil)
	if err != nil {
		return nil, makeGrpcError(err)
	}
	return &pb.PutResponse{}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	store, err := s.getStore(req.DbName)
	if err != nil {
		return nil, err
	}

	err = store.store.Delete(req.Key)
	if err != nil {
		return nil, makeGrpcError(err)
	}
	return &pb.DeleteResponse{}, nil
}

func (s *Server) AtomicPut(ctx context.Context, req *pb.AtomicPutRequest) (*pb.AtomicPutResponse, error) {
	store, err := s.getStore(req.DbName)
	if err != nil {
		return nil, err
	}

	atomicStore, ok := store.store.(cloud.AtomicUnorderedStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "Store does not support atomic ops")
	}

	var previous *cloud.KVPair
	if req.OldVal != nil {
		previous = &cloud.KVPair{
			Key:   req.Key,
			Value: req.OldVal,
		}
	}

	_, _, err = atomicStore.AtomicPut(req.Key, req.Val, previous, nil)
	if err != nil {
		return nil, makeGrpcError(err)
	}
	return &pb.AtomicPutResponse{}, nil
}

func (s *Server) AtomicDelete(ctx context.Context, req *pb.AtomicDeleteRequest) (*pb.AtomicDeleteResponse, error) {
	store, err := s.getStore(req.DbName)
	if err != nil {
		return nil, err
	}

	atomicStore, ok := store.store.(cloud.AtomicUnorderedStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "Store does not support atomic ops")
	}

	if req.OldVal == nil {
		return nil, status.Error(codes.InvalidArgument, "old_val must be non-nil")
	}

	previous := &cloud.KVPair{
		Key:   req.Key,
		Value: req.OldVal,
	}

	_, err = atomicStore.AtomicDelete(req.Key, previous)
	if err != nil {
		return nil, makeGrpcError(err)
	}
	return &pb.AtomicDeleteResponse{}, nil
}

func (s *Server) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	store, err := s.getStore(req.DbName)
	if err != nil {
		return nil, err
	}

	ordered, ok := store.store.(cloud.OrderedStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "List not implemented")
	}

	keys, err := ordered.ListKeys(req.StartKey)
	if err != nil {
		return nil, makeGrpcError(err)
	}

	resp := &pb.ListResponse{
		Keys: keys,
	}
	return resp, nil
}
