package gokv

import (
	"bytes"
	"context"
	"encoding/gob"
	"log"

	"github.com/yixinin/gokv/impls/snap"
	"github.com/yixinin/gokv/kvstore"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type KvEngine struct {
	kvstore.Kvstore
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
		Kvstore:     db,
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

type kv struct {
	Key string
	Val string
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
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("raftexample: could not decode message (%v)", err)
			}
			s.Set(context.Background(), []byte(dataKv.Key), []byte(dataKv.Val))
		}
		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *KvEngine) Propose(k string, v string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.String()
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
	return s.RecoverFromSnapshot(context.Background(), snapshot)
}
