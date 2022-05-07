package gokv

import (
	"context"
	"log"
	"runtime/debug"
	"time"

	"github.com/yixinin/gokv/kvstore"
)

type _ttlImpl struct {
	_db kvstore.Kvstore
}

func (t *_ttlImpl) ExpireAt(ctx context.Context, key string, expireAt uint64) error {
	var nowUnix = uint64(time.Now().Unix())
	data, err := t._db.Get(ctx, []byte(key))
	if err != nil {
		return err
	}
	oldExpireAt, s := kvstore.Bytes2Data(data)
	if (oldExpireAt != 0 && oldExpireAt <= nowUnix) ||
		(expireAt != 0 && expireAt <= nowUnix) {
		_ = t._db.Delete(ctx, []byte(key))
		return ErrNotfound
	}
	return t._db.Set(ctx, []byte(key), kvstore.Data2Bytes(s, expireAt))
}

func (t *_ttlImpl) TTL(ctx context.Context, key string) int64 {
	var nowUnix = uint64(time.Now().Unix())
	data, err := t._db.Get(ctx, []byte(key))
	if err != nil {
		return -2
	}
	expireAt, _ := kvstore.Bytes2Data(data)
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
	defer func() {
		if r := recover(); r != nil {
			log.Println(r, string(debug.Stack()))
		}
	}()

	var ticker = time.NewTicker(time.Second)
	defer ticker.Stop()
	var prevKey []byte
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			func() {
				defer recover()

				var nowUnix = uint64(time.Now().Unix())
				var i int

				t._db.Scan(ctx, func(key, data []byte) {
					expireAt, _ := kvstore.Bytes2Data(data)
					if expireAt != 0 && expireAt <= nowUnix {
						_ = t._db.Delete(ctx, key)
					}
					prevKey = key
					i++
				}, 1000, prevKey)

				if i == 0 {
					prevKey = []byte{}
				}
			}()

		}
	}
}
