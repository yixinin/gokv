package storage

import "context"

type Storage interface {
	Set(ctx context.Context, key, val []byte) error
	Get(ctx context.Context, key []byte) ([]byte, error)
	Delete(ctx context.Context, key []byte) error
	Scan(ctx context.Context, f func(key, data []byte), limit int, prefix []byte)
}
