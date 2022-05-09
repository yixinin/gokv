package wal

import (
	"github.com/yixinin/gokv/impls/wal/walpb"
	"github.com/yixinin/gokv/kvstore"
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

func ValidSnapshotEntries(dir string) ([]walpb.Snapshot, error) {
	return []walpb.Snapshot{}, nil
}

func Create(logger any, dir string, data []byte) (WAL, error) {
	return nil, nil
}
func Open(logger any, dir string, snap walpb.Snapshot) (WAL, error) {
	return nil, nil
}

type wal struct {
	db kvstore.Kvstore
}

func (w *wal) SaveSnapshot(snap walpb.Snapshot) error {

}
func (w *wal) ReleaseLockTo(index uint64) error {

}
func (w *wal) Save(hs raftpb.HardState, entries []raftpb.Entry) {

}
func (w *wal) ReadAll() (metadata []byte, state raftpb.HardState, ents []raftpb.Entry, err error) {

}
func (w *wal) Close() error {

}
