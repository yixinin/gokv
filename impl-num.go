package gokv

import (
	"context"

	"github.com/yixinin/gokv/codec"
	"github.com/yixinin/gokv/kverror"
	"github.com/yixinin/gokv/kvstore"
	"github.com/yixinin/gokv/redis/protocol"
)

type _numImpl struct {
	kv kvstore.Kvstore
}

func NewNumImpl(kv kvstore.Kvstore) *_numImpl {
	return &_numImpl{
		kv: kv,
	}
}

func (n *_numImpl) Incr(ctx context.Context, cmd *protocol.IncrByCmd) *Submit {
	data, err := n.kv.Get(ctx, cmd.Key)
	if err != nil && err != kverror.ErrNotFound {
		cmd.Err = err
		return nil
	}
	oldV := codec.Decode(data)
	// set new
	if oldV.Expired(cmd.Now) || err == kverror.ErrNotFound {
		return NewSetRawSubmit(cmd.Key, codec.EncodeInt(cmd.Val).Raw())
	}

	// incr
	sumB := bytesAdd(data[9:], codec.Int642Bytes(cmd.Val))

	cmd.Val = codec.Bytes2Int64(sumB)
	return NewSetRawSubmit(cmd.Key, data)
}

func bytesAdd(b1, b2 []byte) []byte {
	if len(b1) != len(b2) || len(b1) == 0 {
		panic("cannot add bytes with ne size")
	}
	var overflow bool
	var b1i byte
	for i := len(b1) - 1; i >= 0; i-- {
		preOverflow := overflow
		b1i = b1[i]
		b1[i] = b1[i] + b2[i]
		if preOverflow {
			b1[i]++
		}
		if b1i == 0 || b2[i] == 0 {
			overflow = b1[i] < b1i || b1[i] < b2[i]
		} else {
			overflow = b1[i] <= b1i || b1[i] <= b2[i]
		}

	}
	return b1
}
