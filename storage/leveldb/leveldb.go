package leveldb

import (
	"context"

	"github.com/syndtr/goleveldb/leveldb"
)

type ldb struct {
	db *leveldb.DB
}

func (l *ldb) Set(ctx context.Context, key, val []byte) error {
	return l.db.Put(key, val, nil)
}

func (l *ldb) Get(ctx context.Context, key []byte) ([]byte, error) {
	return l.db.Get(key, nil)
}
func (l *ldb) Delete(ctx context.Context, key []byte) error {
	return l.db.Delete(key, nil)
}
