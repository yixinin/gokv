package leveldb

import (
	"context"
	"encoding/json"
	"os"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/tiglabs/raft/logger"
	"github.com/yixinin/gokv/kverror"
)

type ldb struct {
	db  *leveldb.DB
	dir string
}

func (l *ldb) Set(ctx context.Context, key, val []byte) error {
	logger.Debug("%vset %s: %s", l, key, val)
	return l.db.Put(key, val, nil)
}

func (l *ldb) Get(ctx context.Context, key []byte) ([]byte, error) {
	logger.Debug("%vget %s:", l, key)
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

func (l *ldb) GetSnapshot(ctx context.Context) ([]byte, error) {
	ss, err := l.db.GetSnapshot()
	defer ss.Release()
	if err != nil {
		return nil, err
	}
	var m = make(map[string][]byte, 16)
	iter := ss.NewIterator(nil, nil)
	for iter.Next() {
		m[string(iter.Key())] = iter.Value()
	}
	return json.Marshal(m)
}

func (m *ldb) RecoverFromSnapshot(ctx context.Context, data []byte) error {
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

func (m *ldb) clearAndReopen(ctx context.Context) error {
	if err := m.db.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(m.dir); err != nil {
		return err
	}
	var err error
	m.db, err = leveldb.OpenFile(m.dir, nil)
	return err
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
