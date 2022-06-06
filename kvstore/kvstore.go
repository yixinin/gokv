package kvstore

import (
	"context"

	"github.com/yixinin/gokv/kvstore/leveldb"
	"github.com/yixinin/gokv/kvstore/memdb"
)

type Kvstore interface {
	Set(ctx context.Context, key, val []byte) error
	Get(ctx context.Context, key []byte) ([]byte, error)
	Delete(ctx context.Context, key []byte) error
	Scan(ctx context.Context, f func(key, data []byte), skip, limit int, prefix []byte) uint64
	Close(ctx context.Context) error
}

var NewMemDB = memdb.NewStorage
var NewLevelDB = leveldb.NewStorage
