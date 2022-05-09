package walpb

import "go.etcd.io/etcd/raft/v3/raftpb"

type Snapshot struct {
	Index     uint64
	Term      uint64
	ConfState *raftpb.ConfState
}

func (s Snapshot) Close() {
}
