package leveldb

import (
	"context"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
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
func (l *ldb) Scan(ctx context.Context, f func(key, data []byte), limit int, prefix []byte) {
	slice := util.BytesPrefix(prefix)
	iter := l.db.NewIterator(slice, nil)
	defer iter.Release()
	for i := 0; iter.Next() && i < limit; i++ {
		f(iter.Key(), iter.Value())
	}
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
