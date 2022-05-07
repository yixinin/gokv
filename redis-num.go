package gokv

import (
	"context"
	"strconv"

	"github.com/yixinin/gokv/kvstore"
)

type _numImpl struct {
	_db kvstore.Kvstore
}

func (n *_numImpl) Incr(ctx context.Context, key string, val string) (string, error) {
	data, err := n._db.Get(ctx, []byte(key))
	if err != nil {
		if err == ErrNotfound {
			data := kvstore.Data2Bytes(val, 0)
			if data[0] != kvstore.IntType {
				return "", ErrValOpType
			}
			err := n._db.Set(ctx, []byte(key), data)
			return val, err
		}
		return "", err
	}
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return "", ErrValOpType
	}
	sumB := bytesAdd(data[9:], kvstore.Int642Bytes(i))
	err = n._db.Set(ctx, []byte(key), data)
	if err != nil {
		return "", err
	}
	sum := kvstore.Bytes2Int64(sumB)
	return strconv.FormatInt(sum, 10), err
}

func bytesAdd(b1, b2 []byte) []byte {
	if len(b1) != len(b2) || len(b1) == 0 {
		panic("cannot add bytes with ne size")
	}
	var overflow bool
	var b1i byte
	for i := len(b1) - 1; i >= 0; i-- {
		b1i = b1[i]
		b1[i] = b1[i] + b2[i]
		if overflow {
			b1[i]++
		}
		overflow = b1[i] < b1i || b1[i] < b2[i]
	}
	return b1
}
