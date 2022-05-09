package wal

import (
	"github.com/yixinin/gokv/wal/walpb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type WAL interface {
	SaveSnapshot(snap walpb.Snapshot) error
	ReleaseLockTo(index uint64) error
	Save(hs raftpb.HardState, entries []raftpb.Entry)
	ReadAll() (metadata []byte, state raftpb.HardState, ents []raftpb.Entry, err error)
	Close() error
}

func Exist(dir string) bool {
	return false
}

func ValidSnapshotEntries(dir string) (walpb.Snapshot, error) {
	return walpb.Snapshot{}, nil
}

func Create(logger any, dir string, data []byte) (WAL, error) {
	return nil, nil
}
func Open(logger any, dir string, snap walpb.Snapshot) (WAL, error) {
	return nil, nil
}
