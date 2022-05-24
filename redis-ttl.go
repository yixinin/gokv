package gokv

import (
	"context"
	"log"
	"runtime/debug"
	"time"

	"github.com/yixinin/gokv/codec"
)

type _ttlImpl struct {
	cmd *CmdContainer
}

func (t *_ttlImpl) ExpireAt(ctx context.Context, key string, expireAt uint64) error {
	var nowUnix = uint64(time.Now().Unix())
	data, err := t.cmd.Get(ctx, key)
	if err != nil {
		return err
	}
	v := codec.Decode(data)
	oldExpireAt := v.ExpireAt()
	if (oldExpireAt != 0 && oldExpireAt <= nowUnix) ||
		(expireAt != 0 && expireAt <= nowUnix) {
		_ = t.cmd.Delete(ctx, key)
		return ErrNotfound
	}
	v.SetExpireAt(expireAt)
	return t.cmd.SetRaw(ctx, key, v.Raw())
}

func (t *_ttlImpl) TTL(ctx context.Context, key string) int64 {
	var nowUnix = uint64(time.Now().Unix())
	data, err := t.cmd.Get(ctx, key)
	if err != nil {
		return -2
	}
	v := codec.Decode(data)
	expireAt := v.ExpireAt()
	if expireAt == 0 {
		return -1
	}
	if expireAt != 0 && expireAt <= nowUnix {
		_ = t.cmd.Delete(ctx, key)
		return -2
	}
	return int64(expireAt - nowUnix)
}

func (t *KvEngine) GC(ctx context.Context) {
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
				cmd, commit := t.BaseCmd()
				defer commit()
				var nowUnix = uint64(time.Now().Unix())
				var i int

				t._kv.Scan(ctx, func(key, data []byte) {
					expireAt := codec.Decode(data).ExpireAt()
					if expireAt != 0 && expireAt <= nowUnix {
						_ = cmd.Delete(ctx, codec.BytesToString(key))
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
