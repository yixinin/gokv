package leveldb

import (
	"context"
	"math"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/yixinin/gokv/codec"
	"github.com/yixinin/gokv/kverror"
)

type ldb struct {
	db  *leveldb.DB
	dir string
}

func (l *ldb) Set(ctx context.Context, key, val []byte) error {
	return l.db.Put(key, val, nil)
}

func (l *ldb) Get(ctx context.Context, key []byte) ([]byte, error) {
	data, err := l.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil, kverror.ErrNotFound
	}
	return data, err
}
func (l *ldb) Delete(ctx context.Context, key []byte) error {
	return l.db.Delete(key, nil)
}
func (l *ldb) Scan(ctx context.Context, f func(key, data []byte), skip, limit int, prefix []byte) uint64 {
	var slice *util.Range
	if prefix != nil {
		slice = util.BytesPrefix(prefix)
	}
	iter := l.db.NewIterator(slice, nil)
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

func (m *ldb) Close(ctx context.Context) error {
	if m != nil && m.db != nil {
		return m.db.Close()
	}
	return nil
}

func NewStorage(path string) (*ldb, error) {
	db, err := leveldb.OpenFile(path, nil)
	return &ldb{db: db, dir: path}, err
}
