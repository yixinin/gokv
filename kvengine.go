package gokv

import (
	"context"
	"encoding/json"
	"log"

	"github.com/yixinin/gokv/codec"
	"github.com/yixinin/gokv/impls/snap"
	"github.com/yixinin/gokv/kvstore"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type KvEngine struct {
	_kv         kvstore.Kvstore
	proposeC    chan<- string // channel for proposing updates
	snapshotter *snap.Snapshotter
}

const (
	MEMDB = "memdb"
)

func NewKvEngine(db kvstore.Kvstore, snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *KvEngine {
	engine := &KvEngine{
		_kv:         db,
		snapshotter: snapshotter,
		proposeC:    proposeC,
	}

	snapshot, err := engine.LoadSnapshot()
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := engine.RecoverFromSnapshot(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}
	// read commits from raft into kvStore map until error
	go engine.readCommits(commitC, errorC)

	return engine
}

type KvCmd struct {
	Key string
	Val []byte
	Del bool
}
type KvCmds []KvCmd

func (cmd KvCmds) Marshal() string {
	buf, _ := json.Marshal(cmd)
	return string(buf)
}

func (s *KvEngine) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		if commit == nil {
			// signaled to load snapshot
			snapshot, err := s.LoadSnapshot()
			if err != nil {
				log.Panic(err)
			}
			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.RecoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			}
			continue
		}

		for _, data := range commit.data {
			var dataKvs KvCmds
			if err := json.Unmarshal([]byte(data), &dataKvs); err != nil {
				log.Fatalf("raftexample: could not decode message (%v)", err)
			}
			for _, dataKv := range dataKvs {
				if dataKv.Del {
					s._kv.Delete(context.Background(), []byte(dataKv.Key))
				} else {
					s._kv.Set(context.Background(), []byte(dataKv.Key), []byte(dataKv.Val))
				}
			}

		}
		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *KvEngine) Propose(cmd ...KvCmd) {
	if len(cmd) == 0 {
		return
	}
	s.proposeC <- KvCmds(cmd).Marshal()
}

func (s *KvEngine) LoadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *KvEngine) RecoverFromSnapshot(snapshot []byte) error {
	return s._kv.RecoverFromSnapshot(context.Background(), snapshot)
}

func (s *KvEngine) Get(ctx context.Context, key []byte) ([]byte, error) {
	return s._kv.Get(ctx, key)
}

func (e *KvEngine) GetSnapshot(ctx context.Context) ([]byte, error) {
	return e._kv.GetSnapshot(ctx)
}
func (s *KvEngine) commit(c *CmdContainer) error {
	s.Propose(c.cmds...)
	return nil
}

type CmdContainer struct {
	cmds KvCmds
	db   kvstore.Kvstore
}

func (s *KvEngine) BaseCmd() (*_baseImpl, func()) {
	var cmd = &CmdContainer{
		db:   s._kv,
		cmds: make(KvCmds, 0, 1),
	}
	var commit = func() {
		s.commit(cmd)
	}
	return &_baseImpl{
		cmd: cmd,
	}, commit
}

func (s *KvEngine) TTLCmd() (*_ttlImpl, func()) {
	var cmd = &CmdContainer{
		cmds: make(KvCmds, 0, 1),
		db:   s._kv,
	}
	var commit = func() {
		s.commit(cmd)
	}
	return &_ttlImpl{
		cmd: cmd,
	}, commit
}

func (s *KvEngine) HashCmd() (*_hashImpl, func()) {
	var cmd = &CmdContainer{
		cmds: make(KvCmds, 0, 1),
		db:   s._kv,
	}
	var commit = func() {
		s.commit(cmd)
	}
	return &_hashImpl{
		cmd: cmd,
	}, commit
}

func (c *CmdContainer) Set(ctx context.Context, key, val string, expireAt ...uint64) error {
	c.cmds = append(c.cmds, KvCmd{
		Key: key,
		Val: codec.Encode(val, expireAt...).Raw(),
	})

	return nil
}

func (c *CmdContainer) SetRaw(ctx context.Context, key string, data []byte) error {
	c.cmds = append(c.cmds, KvCmd{
		Key: key,
		Val: data,
	})
	return nil
}
func (c *CmdContainer) Delete(ctx context.Context, key string) error {
	c.cmds = append(c.cmds, KvCmd{
		Key: key,
		Del: true,
	})
	return nil
}
func (c *CmdContainer) Get(ctx context.Context, key string) ([]byte, error) {
	return c.db.Get(ctx, codec.StringToBytes(key))
}
