package memdb

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/yixinin/gokv"
	"github.com/yixinin/gokv/kvstore"
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

func (m *memdb) Scan(ctx context.Context, f func(key, data []byte), limit int, prefix []byte) {
	var i int
	m.m.Range(func(key, value any) bool {
		f([]byte(key.(string)), value.([]byte))
		i++
		return i < limit
	})
}

func (m *memdb) GetSnapshot(ctx context.Context) ([]byte, error) {
	var datas = make(map[string][]byte, 16)
	m.m.Range(func(key, value any) bool {
		datas[key.(string)] = value.([]byte)
		return false
	})
	return json.Marshal(datas)
}

func NewStorage() kvstore.KvStore {
	return &memdb{}
}
