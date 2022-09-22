package cloud

import (
	"github.com/docker/libkv/store"
)

var (
	ErrKeyNotFound = store.ErrKeyNotFound
	ErrKeyExists   = store.ErrKeyExists
	ErrKeyModified = store.ErrKeyModified
)

type (
	KVPair = store.KVPair
)
