package mem

import (
	"context"
	"errors"
	"sync"
)

type memdb struct {
	m sync.Map
}

func (m *memdb) Set(ctx context.Context, key, val []byte) error {
	m.m.Store(key, val)
	return nil
}

func (m *memdb) Get(ctx context.Context, key []byte) ([]byte, error) {
	val, ok := m.m.Load(key)
	if !ok {
		return nil, errors.New("not found")
	}
	return val.([]byte), nil
}

func (m *memdb) Delete(ctx context.Context, key []byte) error {
	m.m.Delete(key)
	return nil
}
