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
	if cmd.DEL {
		return s.Delete(ctx, cmd.BaseCmd)
	}

	if cmd.NX || cmd.KEEPEX {
		switch data, err := s.kv.Get(ctx, cmd.Key); err {
		case nil:
			val := codec.Decode(data)
			if cmd.NX {
				if !val.Expired(cmd.Now) {
					cmd.Err = kverror.ErrNIL
					return nil
				}
			}

			if cmd.KEEPEX && !val.Expired(cmd.Now) {
				cmd.EX = val.ExpireAt()
			}
		case kverror.ErrNotFound:
			// do nothing
		default:
			cmd.Err = err
			return nil
		}
	}
	return NewSetSubmit(cmd.Key, cmd.Val, cmd.EX)
}

func (s *_baseImpl) Get(ctx context.Context, cmd *protocol.GetCmd) {
	data, err := s.kv.Get(ctx, cmd.Key)
	if err != nil {
		cmd.Err = err
		return
	}
	v := codec.Decode(data)
	if v.Expired(cmd.Now) {
		cmd.Err = kverror.ErrNIL
		return
	}
	cmd.Val = v.StringVal()
	return
}

func (s *_baseImpl) Delete(ctx context.Context, cmd *protocol.BaseCmd) *Submit {
	cm := NewDelSubmit(cmd.Key)
	return cm
}
