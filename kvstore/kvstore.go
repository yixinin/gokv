package kvstore

import "context"

type Kvstore interface {
	Set(ctx context.Context, key, val []byte) error
	Get(ctx context.Context, key []byte) ([]byte, error)
	Delete(ctx context.Context, key []byte) error
	Scan(ctx context.Context, f func(key, data []byte), limit int, prefix []byte)
	GetSnapshot(ctx context.Context) ([]byte, error)
	RecoverFromSnapshot(ctx context.Context, data []byte) error
}

var NewLevelDB func(string) (Kvstore, error)
var NewMemDB func() Kvstore
