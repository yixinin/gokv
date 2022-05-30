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

func (t *_ttlImpl) ExpireAt(ctx context.Context, cmd *protocol.ExpireCmd) *Commit {
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
		return NewDelCommit(cmd.Key)
	}

	v.SetExpireAt(cmd.EX)

	return NewSetRawCommit(cmd.Key, v.Raw())
}

func (t *_ttlImpl) TTL(ctx context.Context, ttl *protocol.TTLCmd) *Commit {
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
		return NewDelCommit(ttl.Key)
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
	var prevKey []byte
loop:
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if t.leader != t.nodeID {
				continue loop
			}
			var batch = 10
			func() {
				defer recover()
				commit, _ := t.StartCommit(ctx)
				var nowUnix = uint64(time.Now().Unix())
				var i int
				var cts = make([]*Commit, 0, batch)
				t.db.Scan(ctx, func(key, data []byte) {
					if codec.Decode(data).Expired(nowUnix) {
						cts = append(cts, NewDelCommit(key))
					}
					prevKey = key
					i++
				}, batch, prevKey)

				if i == 0 {
					prevKey = []byte{}
				}
				commit(cts...)
			}()

		}
	}
}
