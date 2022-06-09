// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.package wal

package gokv

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/yixinin/gokv/codec"
	"github.com/yixinin/gokv/kverror"
	"github.com/yixinin/gokv/kvstore"
	"github.com/yixinin/gokv/logger"
	"github.com/yixinin/raft"
	"github.com/yixinin/raft/proto"
	"github.com/yixinin/raft/storage/wal"
)

// DefaultClusterID the default cluster id, we have only one raft cluster
const DefaultClusterID = 1

// DefaultRequestTimeout default request timeout
const DefaultRequestTimeout = time.Second * 3

type AppliedIndex int

func (i AppliedIndex) UniqueKey() string {
	return strconv.Itoa(int(i))
}

// RaftKv the kv server
type RaftKv struct {
	cfg    *Config
	nodeID uint64       // self node id
	node   *ClusterNode // self node
	leader uint64

	// hs *http.Server
	rs *raft.RaftServer

	fs *os.File

	*_baseImpl
	*_numImpl
	*_ttlImpl
	db kvstore.Kvstore // we use leveldb to store key-value data
}

// NewRaftKv create kvs
func NewRaftKv(nodeID uint64, cfg *Config) *RaftKv {
	s := &RaftKv{
		nodeID: nodeID,
		cfg:    cfg,
	}
	node := cfg.FindClusterNode(nodeID)
	if node == nil {
		logger.Errorf(context.TODO(), "could not find self node(%v) in cluster config: \n(%v)", nodeID, cfg.String())
	}
	s.node = node
	return s
}

// Run run server
func (s *RaftKv) Run(ctx context.Context) {
	// init store
	s.initLeveldb(ctx)
	// start raft
	s.startRaft(ctx)
}

// Stop stop server
func (s *RaftKv) Stop(ctx context.Context) {

	// stop raft server
	if s.rs != nil {
		s.rs.Stop()
	}

	// close leveldb
	if s.db != nil {
		if err := s.db.Close(ctx); err != nil {
			logger.Errorf(ctx, "close leveldb failed: %v", err)
		}
	}
}

func (s *RaftKv) initLeveldb(ctx context.Context) {
	if s.cfg.ServerCfg.DataPath == "memdb" {
		s.db = kvstore.NewMemDB()
		return
	}
	dbPath := path.Join(s.cfg.ServerCfg.DataPath, "db")
	db, err := kvstore.NewLevelDB(dbPath)
	if err != nil {
		logger.Errorf(ctx, "init leveldb failed: %v, path: %v", err, dbPath)
		panic(err)
	}
	s.db = db
	s._baseImpl = NewBaseImpl(s.db)
	s._numImpl = NewNumImpl(s.db)
	s._ttlImpl = NewTTLImpl(s.db)
	logger.Infof(ctx, "init leveldb sucessfully. path: %v", dbPath)

	idxPath := path.Join(s.cfg.ServerCfg.DataPath, "applied.index")
	fs, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		panic(err)
	}
	s.fs = fs
}

func (s *RaftKv) startRaft(ctx context.Context) {
	// start raft server
	sc := raft.DefaultConfig()
	sc.NodeID = s.nodeID
	sc.Resolver = newClusterResolver(s.cfg)
	sc.TickInterval = time.Millisecond * 500
	sc.ReplicateAddr = fmt.Sprintf(":%d", s.node.ReplicatePort)
	sc.HeartbeatAddr = fmt.Sprintf(":%d", s.node.HeartbeatPort)
	rs, err := raft.NewRaftServer(sc)

	if err != nil {
		logger.Errorf(ctx, "start raft server failed: %v", err)
		panic(err)
	}
	s.rs = rs
	logger.Info(ctx, "raft server started.")

	// create raft
	walPath := path.Join(s.cfg.ServerCfg.DataPath, "wal")
	raftStore, err := wal.NewStorage(walPath, &wal.Config{})
	if err != nil {
		logger.Errorf(ctx, "init raft log storage failed: %v", err)
		panic(err)
	}
	rc := &raft.RaftConfig{
		ID:           DefaultClusterID,
		Storage:      raftStore,
		StateMachine: s,
		Applied:      s.getAppliedIndex(),
	}
	for _, n := range s.cfg.ClusterCfg.Nodes {
		rc.Peers = append(rc.Peers, proto.Peer{
			Type:   proto.PeerNormal,
			ID:     n.NodeID,
			PeerID: n.NodeID,
		})
	}
	err = s.rs.CreateRaft(rc)
	if err != nil {
		logger.Errorf(ctx, "create raft failed: %v", err)
		panic(err)
	}
	logger.Info(ctx, "raft created.")
}

// Apply implement raft StateMachine Apply method
func (s *RaftKv) Apply(command []byte, index uint64) (interface{}, error) {
	var submits []*Submit
	err := json.Unmarshal(command, &submits)
	if err != nil {
		return false, fmt.Errorf("unmarshal command failed: %v", command)
	}

	for _, submit := range submits {
		err := s.apply(context.Background(), submit, index)
		if err != nil {
			return false, err
		}
		go s.updateAppliedIndex(index)
	}
	return true, nil
}

func (s *RaftKv) updateAppliedIndex(index uint64) {
	s.fs.Seek(0, 0)
	s.fs.Write(codec.Uint642Bytes(index))
}

func (s *RaftKv) getAppliedIndex() uint64 {
	var b = make([]byte, 8)
	n, _ := s.fs.Read(b)
	if n != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func (s *RaftKv) apply(ctx context.Context, cmd *Submit, index uint64) error {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf(ctx, "apply set [%s %v] error:%v, stacks:%s", cmd.Key, cmd.Value, r, debug.Stack())
		}
	}()
	switch cmd.OP {
	case CommitOPSet:
		if logger.EnableDebug() && s.leader != s.nodeID {
			v := codec.Decode(cmd.Value)
			val := v.String()
			ex := v.ExpireAt()
			if ex > 0 {
				logger.Debugf(ctx, "apply set command at index(%v) key:%s : %v, ex:%s", index, cmd.Key, val, time.Unix(int64(ex), 0).Local().Format(time.Stamp))
			} else {
				logger.Debugf(ctx, "apply set command at index(%v) key:%s : %v, long live", index, cmd.Key, val)
			}
		}
		err := s.db.Set(ctx, cmd.Key, cmd.Value)
		if err != nil {
			logger.Errorf(ctx, "apply set [%s %v] error:%v", cmd.Key, cmd.Value, err)
		}
		return err
	case CommitOPDel:
		if logger.EnableDebug() && s.leader != s.nodeID {
			logger.Debugf(ctx, "apply del command at index(%v) key:%s", index, cmd.Key)
		}
		err := s.db.Delete(ctx, cmd.Key)
		if err != nil {
			logger.Errorf(ctx, "apply del [%s] error:%v", cmd.Key, err)
		}
		return err
	case CommitOPExDel:
		if logger.EnableDebug() && s.leader != s.nodeID {
			logger.Debugf(ctx, "apply exdel command at index(%v) key:%s", index, cmd.Key)
		}
		data, err := s.db.Get(ctx, cmd.Key)
		if err != nil {
			if errors.Is(err, kverror.ErrNotFound) {
				return nil
			}
			return err
		}
		if codec.Decode(data).Expired(uint64(time.Now().Unix())) {
			err := s.db.Delete(ctx, cmd.Key)
			if err != nil {
				logger.Errorf(ctx, "apply exdel [%s] error:%v", cmd.Key, err)
			}
			return err
		}
	}
	return nil
}

// ApplyMemberChange implement raft.StateMachine
func (s *RaftKv) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	return nil, errors.New("not supported")
}

// Snapshot implement raft.StateMachine
func (s *RaftKv) Snapshot() (proto.Snapshot, error) {
	// return s.db.GetSnapshot(context.Background())
	return nil, errors.New("not supported")
}

// ApplySnapshot implement raft.StateMachine
func (s *RaftKv) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	// s.db.RecoverFromSnapshot(context)
	return errors.New("not supported")
}

// HandleFatalEvent implement raft.StateMachine
func (s *RaftKv) HandleFatalEvent(err *raft.FatalError) {
	logger.Errorf(context.TODO(), "raft fatal error: %v", err)
	panic(err)
}

// HandleLeaderChange implement raft.StateMachine
func (s *RaftKv) HandleLeaderChange(leader uint64) {
	logger.Infof(context.TODO(), "raft leader change to %v", leader)
	s.leader = leader
}

func (s *RaftKv) StartSubmit(ctx context.Context) (func(submits ...*Submit) (bool, error), bool) {
	var submit = func(submits ...*Submit) (bool, error) {
		if len(submits) == 0 {
			return false, nil
		}
		defer func() {
			if r := recover(); r != nil {
				buf, _ := json.Marshal(submits)
				logger.Errorf(ctx, "recovered from submit:%s err:%v stacks:%s", buf, r, debug.Stack())
			}
		}()
		return s.process(ctx, submits...)
	}
	if s.leader != s.nodeID {
		return submit, false
	}
	return submit, true
}

func (s *RaftKv) SubmitAsync(submits ...*Submit) {
	go s.process(context.Background(), submits...)
}

func (s *RaftKv) getLeader() *ClusterNode {
	return s.cfg.FindClusterNode(s.leader)
}

func (s *RaftKv) process(ctx context.Context, submits ...*Submit) (ok bool, err error) {
	if len(submits) == 0 || submits[0] == nil {
		return
	}
	data, err := json.Marshal(submits)
	if err != nil {
		logger.Errorf(ctx, "marshal raft command failed: %v", err)
		return
	}

	f := s.rs.Submit(DefaultClusterID, data)
	respCh, errCh := f.AsyncResponse()
	select {
	case resp := <-respCh:
		ok, _ = resp.(bool)
		return
	case err = <-errCh:
		return
	case <-time.After(DefaultRequestTimeout):
		err = os.ErrDeadlineExceeded
		return
	}
}

func (s *RaftKv) getByReadIndex(ctx context.Context, key string) ([]byte, error) {
	f := s.rs.ReadIndex(DefaultClusterID)
	respCh, errCh := f.AsyncResponse()
	select {
	case resp := <-respCh:
		if resp != nil {
			return nil, fmt.Errorf("process get %s failed: unexpected resp type: %T", key, resp)
		}
		value, err := s.db.Get(ctx, []byte(key))
		return value, err

	case err := <-errCh:
		return nil, err
	case <-time.After(DefaultRequestTimeout):
		return nil, os.ErrDeadlineExceeded
	}
}
