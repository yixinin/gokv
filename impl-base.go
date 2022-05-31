package gokv

import (
	"context"

	"github.com/yixinin/gokv/codec"
	"github.com/yixinin/gokv/kverror"
	"github.com/yixinin/gokv/kvstore"
	"github.com/yixinin/gokv/redis/protocol"
)

type _baseImpl struct {
	kv kvstore.Kvstore
}

func NewBaseImpl(kv kvstore.Kvstore) *_baseImpl {
	return &_baseImpl{
		kv: kv,
	}
}

func (s *_baseImpl) Set(ctx context.Context, cmd *protocol.SetCmd) *Submit {
	ct := s.checkExpire(ctx, cmd)
	if ct != nil {
		return ct
	}
	if cmd.NX {
		data, err := s.kv.Get(ctx, cmd.Key)
		if err != nil {
			if err != kverror.ErrNotFound {
				cmd.Err = err
				return nil
			}
		} else {
			val := codec.Decode(data)
			if !val.Expired(cmd.Now) {
				cmd.Err = kverror.ErrNIL
				return nil
			}
		}
	}
	ct = NewSetSubmit(cmd.Key, cmd.Val, cmd.EX)
	return ct
}

func (s *_baseImpl) Get(ctx context.Context, cmd *protocol.GetCmd) *Submit {
	data, err := s.kv.Get(ctx, cmd.Key)
	if err != nil {
		cmd.Err = err
		return nil
	}
	v := codec.Decode(data)
	if v.Expired(cmd.Now) {
		return s.Delete(ctx, cmd.BaseCmd)
	}
	cmd.Val = v.StringVal()
	return nil
}

func (s *_baseImpl) Delete(ctx context.Context, cmd *protocol.BaseCmd) *Submit {
	cm := NewDelSubmit(cmd.Key)
	return cm
}

func (s *_baseImpl) checkExpire(ctx context.Context, cmd *protocol.SetCmd) *Submit {
	if cmd.EX != 0 && cmd.EX <= cmd.Now {
		return s.Delete(ctx, cmd.BaseCmd)
	}
	return nil
}
