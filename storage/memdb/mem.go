package memdb

import (
	"context"
	"sync"

	"github.com/yixinin/gokv"
	"github.com/yixinin/gokv/storage"
)

type memdb struct {
	m sync.Map
}

func (m *memdb) Set(ctx context.Context, key, val []byte) error {
	m.m.Store(string(key), val)
	return nil
}

func (m *memdb) Get(ctx context.Context, key []byte) ([]byte, error) {
	val, ok := m.m.Load(string(key))
	if !ok {
		return nil, gokv.ErrNotfound
	}
	return val.([]byte), nil
}

func (m *memdb) Delete(ctx context.Context, key []byte) error {
	m.m.Delete(string(key))
	return nil
}

func NewStorage() storage.Storage {
	return &memdb{}
}
