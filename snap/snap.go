package snap

import (
	"github.com/yixinin/gokv/wal/walpb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Snapshotter struct {
	dir string
}

func (s *Snapshotter) SaveSnap(snap raftpb.Snapshot) error {

}

func (s *Snapshotter) LoadNewestAvailable(walSnaps walpb.Snapshot) (*raftpb.Snapshot, error) {

}

func New(dir string) *Snapshotter {
	return &Snapshotter{}
}
