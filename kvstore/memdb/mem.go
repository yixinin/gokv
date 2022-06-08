package memdb

import (
	"context"
	"math"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/yixinin/gokv/codec"
	"github.com/yixinin/gokv/kverror"
)

type mdb struct {
	db *memdb.DB
}

func (m *mdb) Set(ctx context.Context, key, val []byte) error {
	return m.db.Put(key, val)
}

func (m *mdb) Get(ctx context.Context, key []byte) ([]byte, error) {
	data, err := m.db.Get(key)
	if err == leveldb.ErrNotFound {
		return nil, kverror.ErrNotFound
	}
	return data, err
}
func (m *mdb) Delete(ctx context.Context, key []byte) error {
	return m.db.Delete(key)
}
func (m *mdb) Scan(ctx context.Context, f func(key, data []byte), skip, limit int, prefix []byte) uint64 {
	var slice *util.Range
	if prefix != nil {
		slice = util.BytesPrefix(prefix)
	}
	iter := m.db.NewIterator(slice)
	defer iter.Release()
	if limit <= 0 {
		limit = math.MaxInt
	}
	if skip > 0 {
		for i := 0; i < skip; i++ {
			if !iter.Next() {
				return 0
			}
		}
	}
	var i int
	for i < limit {
		if !iter.Next() {
			return 0
		}
		keyRaw := iter.Key()
		if keyRaw == nil {
			return 0
		}
		valRaw := iter.Value()
		var key = make([]byte, len(keyRaw))
		var val = make([]byte, len(valRaw))
		copy(key, keyRaw)
		copy(val, valRaw)
		f(key, val)
		i++
		if i == limit && iter.Last() {
			if codec.BytesEq(iter.Key(), key) {
				return 0
			}
		}
	}
	return uint64(skip + i)
}

func (m *mdb) Close(ctx context.Context) error {

	return nil
}

func NewStorage() *mdb {
	m := &mdb{}
	m.db = memdb.New(comparer.DefaultComparer, 1024)
	return m
}
