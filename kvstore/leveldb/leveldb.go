package leveldb

import (
	"context"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/yixinin/gokv"
	"github.com/yixinin/gokv/kvstore"
)

type ldb struct {
	db *leveldb.DB
}

func (l *ldb) Set(ctx context.Context, key, val []byte) error {
	return l.db.Put(key, val, nil)
}

func (l *ldb) Get(ctx context.Context, key []byte) ([]byte, error) {
	data, err := l.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil, gokv.ErrNotfound
	}
	return data, err
}
func (l *ldb) Delete(ctx context.Context, key []byte) error {
	return l.db.Delete(key, nil)
}
func (l *ldb) Scan(ctx context.Context, f func(key, data []byte), limit int, prefix []byte) {
	slice := util.BytesPrefix(prefix)
	iter := l.db.NewIterator(slice, nil)
	defer iter.Release()
	for i := 0; iter.Next() && i < limit; i++ {
		f(iter.Key(), iter.Value())
	}
}

func (l *ldb) GetSnapshot(ctx context.Context) ([]byte, error) {
	ss, err := l.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return []byte(ss.String()), nil
}

func NewStorage(path string) (kvstore.KvStore, error) {
	db, err := leveldb.OpenFile(path, nil)
	return &ldb{db: db}, err
}
