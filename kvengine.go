package gokv

import (
	"context"
	"encoding/json"
	"log"

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

func NewKvEngine(kvpath string, snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *KvEngine {
	var db kvstore.Kvstore
	switch kvpath {
	case MEMDB:
		db = kvstore.NewMemDB()
	default:
		var err error
		db, err = kvstore.NewLevelDB(kvpath)
		if err != nil {
			panic(err)
		}
	}
	engine := &KvEngine{
		_kv:         db,
		snapshotter: snapshotter,
		proposeC:    proposeC,
	}

	snapshot, err := engine.loadSnapshot()
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := engine.recoverFromSnapshot(snapshot.Data); err != nil {
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
			snapshot, err := s.loadSnapshot()
			if err != nil {
				log.Panic(err)
			}
			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
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
	s.proposeC <- KvCmds(cmd).Marshal()
}

func (s *KvEngine) getSnapshot() ([]byte, error) {
	return s.getSnapshot()
}

func (s *KvEngine) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *KvEngine) recoverFromSnapshot(snapshot []byte) error {
	return s._kv.RecoverFromSnapshot(context.Background(), snapshot)
}

func (s *KvEngine) Get(ctx context.Context, key []byte) ([]byte, error) {
	return s._kv.Get(ctx, key)
}

func (s *KvEngine) WithHash() *_hashImpl {
	return &_hashImpl{
		_db: &CmdContainer{
			cmds: make(KvCmds, 0, 1),
			db:   s._kv,
		},
	}
}
func (s *KvEngine) handleHash(ctx context.Context, key, field string, val string) error {
	c := s.WithHash()
	err := c.HSet(ctx, key, field, val)
	if err != nil {
		return err
	}
	s.Propose(c._db.cmds...)
	return nil
}

type CmdContainer struct {
	cmds KvCmds
	db   kvstore.Kvstore
}

func (s *KvEngine) WithKvCmds(ctx context.Context) CmdContainer {
	return CmdContainer{}
}
func (c *CmdContainer) Set(ctx context.Context, key []byte, val []byte) error {
	c.cmds = append(c.cmds, KvCmd{
		Key: string(key),
		Val: val,
	})
	return nil
}

func (c *CmdContainer) Delete(ctx context.Context, key []byte) error {
	c.cmds = append(c.cmds, KvCmd{
		Key: string(key),
		Del: true,
	})
	return nil
}
func (c *CmdContainer) Get(ctx context.Context, key []byte) ([]byte, error) {
	return c.db.Get(ctx, key)
}
