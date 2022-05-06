package gokv

import (
	"context"
	"time"

	"github.com/yixinin/gokv/storage"
)

type _ttlImpl struct {
	_db storage.Storage
}

func (t *_ttlImpl) ExpireAt(ctx context.Context, key string, expireAt uint64) error {
	var nowUnix = uint64(time.Now().Unix())
	data, err := t._db.Get(ctx, []byte(key))
	if err != nil {
		return err
	}
	oldExpireAt, s := storage.Bytes2String(data)
	if (oldExpireAt != 0 && oldExpireAt <= nowUnix) ||
		(expireAt != 0 && expireAt <= nowUnix) {
		_ = t._db.Delete(ctx, []byte(key))
		return ErrNotfound
	}
	return t._db.Set(ctx, []byte(key), storage.String2Bytes(s, expireAt))
}

func (t *_ttlImpl) TTL(ctx context.Context, key string) int64 {
	var nowUnix = uint64(time.Now().Unix())
	data, err := t._db.Get(ctx, []byte(key))
	if err != nil {
		return -2
	}
	expireAt, _ := storage.Bytes2String(data)
	if expireAt == 0 {
		return -1
	}
	if expireAt != 0 && expireAt <= nowUnix {
		_ = t._db.Delete(ctx, []byte(key))
		return -2
	}
	return int64(expireAt - nowUnix)
}

func (t *_ttlImpl) GC(ctx context.Context) {
}
