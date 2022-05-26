package gokv

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/yixinin/gokv/codec"
	"github.com/yixinin/gokv/kvstore"
)

type _baseImpl struct {
	kv kvstore.Kvstore
}

func NewBaseImpl(kv kvstore.Kvstore) *_baseImpl {
	return &_baseImpl{
		kv: kv,
	}
}

func (s *_baseImpl) Set(ctx context.Context, key string, val string, expireAt uint64) *Command {
	cmd := s.checkExpire(ctx, key, expireAt)
	if cmd != nil {
		return cmd
	}
	cmd = &Command{
		OP:    CmdSet,
		Key:   []byte(key),
		Value: codec.Encode(val, expireAt).Raw(),
	}
	return cmd
}

func (s *_baseImpl) Get(ctx context.Context, key string) (*Command, redis.Cmder) {
	data, err := s.kv.Get(ctx, []byte(key))
	if err != nil {
		cmd := redis.NewStatusCmd(ctx)
		cmd.SetVal(err.Error())
		return nil, cmd
	}
	v := codec.Decode(data)
	if v.ExpireAt() > uint64(time.Now().Unix()) {
		return s.Delete(ctx, key), nil
	}
	cmd := redis.NewStringCmd(ctx)
	cmd.SetVal(v.String())
	return nil, cmd
}

func (s *_baseImpl) Delete(ctx context.Context, key string) *Command {
	cmd := &Command{
		OP:  CmdDelete,
		Key: []byte(key),
	}
	return cmd
}

func (s *_baseImpl) checkExpire(ctx context.Context, key string, expireAt uint64) *Command {
	var unixNow = uint64(time.Now().Unix())
	if expireAt != 0 && expireAt <= unixNow {
		return s.Delete(ctx, key)
	}
	return nil
}
