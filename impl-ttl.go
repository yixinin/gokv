package gokv

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/yixinin/gokv/codec"
	"github.com/yixinin/gokv/kverror"
	"github.com/yixinin/gokv/kvstore"
	"github.com/yixinin/gokv/logger"
	"github.com/yixinin/gokv/redis/protocol"
)

type _ttlImpl struct {
	kv kvstore.Kvstore
}

func NewTTLImpl(kv kvstore.Kvstore) *_ttlImpl {
	return &_ttlImpl{
		kv: kv,
	}
}

func (t *_ttlImpl) ExpireAt(ctx context.Context, cmd *protocol.ExpireCmd) *Submit {
	data, err := t.kv.Get(ctx, cmd.Key)
	if err != nil {
		cmd.OK = false
		if err != kverror.ErrNotFound {
			cmd.Err = err
		}
		return nil
	}
	v := codec.Decode(data)
	if v.Expired(cmd.Now) {
		cmd.OK = false
		return nil
	}
	if cmd.Del || (cmd.EX > 0 && cmd.Now >= cmd.EX) {
		return NewDelSubmit(cmd.Key)
	}

	v.SetExpireAt(cmd.EX)

	return NewSetRawSubmit(cmd.Key, v.Raw())
}

func (t *_ttlImpl) TTL(ctx context.Context, ttl *protocol.TTLCmd) *Submit {
	data, err := t.kv.Get(ctx, ttl.Key)
	if err != nil {
		if err == kverror.ErrNotFound {
			ttl.TTL = -2
			return nil
		}
		ttl.Err = err
		return nil
	}
	v := codec.Decode(data)
	if v.Expired(ttl.Now) {
		ttl.TTL = -2
		return NewDelSubmit(ttl.Key)
	}

	if v.ExpireAt() == 0 {
		ttl.TTL = -1
		return nil
	}
	ttl.TTL = int64(v.ExpireAt() - ttl.Now)
	return nil
}

func (t *RaftKv) GC(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf(ctx, "ttl gc recovered %v, stacks:%s", r, debug.Stack())
		}
	}()

	var ticker = time.NewTicker(time.Second)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ticker.Stop()
			if t.leader != t.nodeID {
				ticker.Reset(time.Second)
				continue loop
			}
			func() {
				defer recover()
				var nowUnix = uint64(time.Now().Unix())

				var submits = make([]*Submit, 0, 10)
				var f = func(key, data []byte) {
					if v := codec.Decode(data); v.Expired(nowUnix) {
						st := NewDelSubmit(key)
						if logger.EnableDebug() {
							logger.Debugf(ctx, "gc del %s ex:%s, val:%s", key, time.Unix(int64(v.ExpireAt()), 0), v.String())
						}
						submits = append(submits, st)
					}
				}
				t.db.Scan(ctx, f, 0, nil)
				if len(submits) > 0 {
					if logger.EnableDebug() {
						for _, v := range submits {
							logger.Debugf(ctx, "push submit: %s", v.Key)
						}
					}
					t.queue.Push(submits...)
				}
			}()
			ticker.Reset(time.Second)
		}
	}
}
