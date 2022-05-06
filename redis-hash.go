package gokv

import "context"

type RedisHash interface {
	HSet(ctx context.Context, key string, field string, val string) error
}
