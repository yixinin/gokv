package gokv

import (
	"context"

	"github.com/yixinin/gokv/storage"
)

type setImpl struct {
	db storage.Storage
}

func (s *setImpl) Set(ctx context.Context, key string, val string, expireAt uint64) error {
	err := s.db.Set(ctx, []byte(key), storage.String2Bytes(val))
	return err
}
