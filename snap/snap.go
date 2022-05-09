package snap

import (
	"github.com/yixinin/gokv/wal/walpb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Snapshotter struct {
	dir string
}

func (s *Snapshotter) SaveSnap(snap raftpb.Snapshot) error {
	return nil
}

func (s *Snapshotter) LoadNewestAvailable(walSnaps walpb.Snapshot) (*raftpb.Snapshot, error) {
	return nil, nil
}

func New(dir string) *Snapshotter {
	return &Snapshotter{}
}

type Message interface {
}
