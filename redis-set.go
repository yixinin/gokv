package gokv

import (
	"context"
	"time"

	"github.com/yixinin/gokv/codec"
	"github.com/yixinin/gokv/kvstore"
)

type _setImpl struct {
	_db kvstore.Kvstore
}

func (s *_setImpl) Set(ctx context.Context, key string, val string, expireAt uint64) error {
	err := s._db.Set(ctx, []byte(key), codec.Encode(val, expireAt).SavedData())
	return err
}

func (s *_setImpl) Get(ctx context.Context, key string) (string, error) {
	data, err := s._db.Get(ctx, []byte(key))
	if err != nil {
		return "", err
	}
	v := codec.Decode(data)
	expireAt, str := v.ExpireAt(), v.String()
	if err := s.checkExpire(ctx, key, expireAt); err != nil {
		return "", err
	}
	return str, nil
}

func (s *_setImpl) Delete(ctx context.Context, key string) error {
	return s._db.Delete(ctx, []byte(key))
}

func (s *_setImpl) checkExpire(ctx context.Context, key string, expireAt uint64) error {
	var unixNow = uint64(time.Now().Unix())
	if expireAt != 0 && expireAt <= unixNow {
		go s._db.Delete(context.Background(), []byte(key))
		return ErrNotfound
	}
	return nil
}
