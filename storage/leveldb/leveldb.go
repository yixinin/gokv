package leveldb

import (
	"context"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/yixinin/gokv"
	"github.com/yixinin/gokv/storage"
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

func NewStorage(path string) (storage.Storage, error) {
	db, err := leveldb.OpenFile(path, nil)
	return &ldb{db: db}, err
}
