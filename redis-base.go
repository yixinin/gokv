package gokv

import (
	"context"
	"time"

	"github.com/yixinin/gokv/codec"
)

type _baseImpl struct {
	cmd *CmdContainer
}

func (s *_baseImpl) Set(ctx context.Context, key string, val string, expireAt uint64) error {
	return s.cmd.Set(ctx, key, val, expireAt)
}

func (s *_baseImpl) Get(ctx context.Context, key string) (string, error) {
	data, err := s.cmd.Get(ctx, key)
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

func (s *_baseImpl) Delete(ctx context.Context, key string) error {
	s.cmd.Delete(ctx, key)
	return nil
}

func (s *_baseImpl) checkExpire(ctx context.Context, key string, expireAt uint64) error {
	var unixNow = uint64(time.Now().Unix())
	if expireAt != 0 && expireAt <= unixNow {
		go s.Delete(context.Background(), key)
		return ErrNotfound
	}
	return nil
}
