package gokv

import (
	"context"
	"time"

	"github.com/yixinin/gokv/codec"
)

type _setImpl struct {
	_kv KvEngine
}

func (s *_setImpl) Set(ctx context.Context, key string, val string, expireAt uint64) error {
	s._kv.Propose(KvCmd{
		Key: key,
		Val: codec.Encode(val, expireAt).SavedData(),
	})
	return nil
}

func (s *_setImpl) Get(ctx context.Context, key string) (string, error) {
	data, err := s._kv.Get(ctx, []byte(key))
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
	s._kv.Propose(KvCmd{
		Key: key,
		Del: true,
	})
	return nil
}

func (s *_setImpl) checkExpire(ctx context.Context, key string, expireAt uint64) error {
	var unixNow = uint64(time.Now().Unix())
	if expireAt != 0 && expireAt <= unixNow {
		go s.Delete(context.Background(), key)
		return ErrNotfound
	}
	return nil
}
