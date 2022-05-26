package gokv

// import (
// 	"context"
// 	"strconv"

// 	"github.com/yixinin/gokv/codec"
// 	"github.com/yixinin/gokv/kverror"
// )

// type _numImpl struct {
// 	cmd *CmdContainer
// }

// func (n *_numImpl) Incr(ctx context.Context, key string, val string) (string, error) {
// 	data, err := n.cmd.Get(ctx, key)
// 	if err != nil && err != kverror.ErrNotFound {
// 		return "", err
// 	}
// 	oldV := codec.Decode(data)
// 	// set new
// 	if oldV.Expired() || err == kverror.ErrNotFound {
// 		err := n.cmd.Set(ctx, key, val)
// 		return val, err
// 	}

// 	// incr
// 	i, err := strconv.ParseInt(val, 10, 64)
// 	if err != nil {
// 		return "", kverror.ErrValOpType
// 	}
// 	sumB := bytesAdd(data[9:], codec.Int642Bytes(i))
// 	err = n.cmd.SetRaw(ctx, key, data)
// 	if err != nil {
// 		return "", err
// 	}
// 	sum := codec.Bytes2Int64(sumB)
// 	return strconv.FormatInt(sum, 10), err
// }

// func bytesAdd(b1, b2 []byte) []byte {
// 	if len(b1) != len(b2) || len(b1) == 0 {
// 		panic("cannot add bytes with ne size")
// 	}
// 	var overflow bool
// 	var b1i byte
// 	for i := len(b1) - 1; i >= 0; i-- {
// 		b1i = b1[i]
// 		b1[i] = b1[i] + b2[i]
// 		if overflow {
// 			b1[i]++
// 		}
// 		overflow = b1[i] < b1i || b1[i] < b2[i]
// 	}
// 	return b1
// }
