package memdb

import (
	"context"
	"encoding/json"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/yixinin/gokv/kverror"
)

type mdb struct {
	db *memdb.DB
}

func (m mdb) Set(ctx context.Context, key, val []byte) error {
	return m.db.Put(key, val)
}

func (m mdb) Get(ctx context.Context, key []byte) ([]byte, error) {
	data, err := m.db.Get(key)
	if err == leveldb.ErrNotFound {
		return nil, kverror.ErrNotFound
	}
	return data, err
}
func (m mdb) Delete(ctx context.Context, key []byte) error {
	return m.db.Delete(key)
}
func (m mdb) Scan(ctx context.Context, f func(key, data []byte), limit int, prefix []byte) {
	slice := util.BytesPrefix(prefix)
	iter := m.db.NewIterator(slice)
	defer iter.Release()
	for i := 0; iter.Next() && i < limit; i++ {
		f(iter.Key(), iter.Value())
	}
}

func (db mdb) GetSnapshot(ctx context.Context) ([]byte, error) {
	var m = make(map[string][]byte, 16)
	iter := db.db.NewIterator(nil)
	for iter.Next() {
		m[string(iter.Key())] = iter.Value()
	}
	return json.Marshal(m)
}

func (m mdb) RecoverFromSnapshot(ctx context.Context, data []byte) error {
	var datas = make(map[string][]byte)
	err := json.Unmarshal(data, &datas)
	if err != nil {
		return err
	}
	if err := m.clearAndReopen(ctx); err != nil {
		return err
	}
	for k, val := range datas {
		m.Set(ctx, []byte(k), val)
	}
	return nil
}

func (m mdb) clearAndReopen(ctx context.Context) error {

	m.db = memdb.New(comparer.DefaultComparer, 1024)
	return nil
}

func (m mdb) Close(ctx context.Context) error {

	return nil
}

func NewStorage() *mdb {
	m := &mdb{}
	m.db = memdb.New(comparer.DefaultComparer, 1024)
	return m
}
