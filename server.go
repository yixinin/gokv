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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
	"github.com/tiglabs/raft/util/log"
	"github.com/yixinin/gokv/codec"
	"github.com/yixinin/gokv/kvstore"
)

// DefaultClusterID the default cluster id, we have only one raft cluster
const DefaultClusterID = 1

// DefaultRequestTimeout default request timeout
const DefaultRequestTimeout = time.Second * 3

// Server the kv server
type Server struct {
	cfg    *Config
	nodeID uint64       // self node id
	node   *ClusterNode // self node
	leader uint64

	hs *http.Server
	rs *raft.RaftServer

	*_baseImpl
	*_netImpl
	db kvstore.Kvstore // we use leveldb to store key-value data
}

// NewServer create kvs
func NewServer(nodeID uint64, cfg *Config) *Server {
	s := &Server{
		nodeID: nodeID,
		cfg:    cfg,
	}
	node := cfg.FindClusterNode(nodeID)
	if node == nil {
		log.Panic("could not find self node(%v) in cluster config: \n(%v)", nodeID, cfg.String())
	}
	s.node = node
	return s
}

// Run run server
func (s *Server) Run() {
	// init store
	s.initLeveldb()
	// start raft
	s.startRaft()
	// start tcp server
	s.initTcp()
}

func (s *Server) initTcp() {
	node := s.cfg.FindClusterNode(s.nodeID)
	s._netImpl.listen(context.Background(), uint16(node.HTTPPort))
}

// Stop stop server
func (s *Server) Stop(ctx context.Context) {
	// stop http server
	if s.hs != nil {
		if err := s.hs.Shutdown(nil); err != nil {
			log.Error("shutdown http failed: %v", err)
		}
	}

	// stop raft server
	if s.rs != nil {
		s.rs.Stop()
	}

	// close leveldb
	if s.db != nil {
		if err := s.db.Close(ctx); err != nil {
			log.Error("close leveldb failed: %v", err)
		}
	}
}

func (s *Server) initLeveldb() {
	if s.cfg.ServerCfg.DataPath == "memdb" {
		s.db = kvstore.NewMemDB()
		return
	}
	dbPath := path.Join(s.cfg.ServerCfg.DataPath, "db")
	db, err := kvstore.NewLevelDB(dbPath)
	if err != nil {
		log.Panic("init leveldb failed: %v, path: %v", err, dbPath)
	}
	s.db = db
	s._netImpl = NewNetImpl(s)
	s._baseImpl = NewBaseImpl(s.db)
	log.Info("init leveldb sucessfully. path: %v", dbPath)
}

func (s *Server) startRaft() {
	// logger.SetLogger(log.GetFileLogger())

	// start raft server
	sc := raft.DefaultConfig()
	sc.NodeID = s.nodeID
	sc.Resolver = newClusterResolver(s.cfg)
	sc.TickInterval = time.Millisecond * 500
	sc.ReplicateAddr = fmt.Sprintf(":%d", s.node.ReplicatePort)
	sc.HeartbeatAddr = fmt.Sprintf(":%d", s.node.HeartbeatPort)
	rs, err := raft.NewRaftServer(sc)
	if err != nil {
		log.Panic("start raft server failed: %v", err)
	}
	s.rs = rs
	log.Info("raft server started.")

	// create raft
	walPath := path.Join(s.cfg.ServerCfg.DataPath, "wal")
	raftStore, err := wal.NewStorage(walPath, &wal.Config{})
	if err != nil {
		log.Panic("init raft log storage failed: %v", err)
	}
	rc := &raft.RaftConfig{
		ID:           DefaultClusterID,
		Storage:      raftStore,
		StateMachine: s,
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
		log.Panic("create raft failed: %v", err)
	}
	log.Info("raft created.")
}

// Apply implement raft StateMachine Apply method
func (s *Server) Apply(command []byte, index uint64) (interface{}, error) {

	var cmds []Command
	err := json.Unmarshal(command, &cmds)
	if err != nil {
		return nil, fmt.Errorf("unmarshal command failed: %v", command)
	}

	var ctx = context.Background()
	for _, cmd := range cmds {
		log.Debug("apply command at index(%v):%s -> %s", index, cmd.OP, cmd.Key)
		if cmd.OP != CmdDelete {
			v := codec.Decode(cmd.Value)
			log.Debug("val: %s,%d", v.String(), v.ExpireAt())
		}
		switch cmd.OP {
		case CmdSet:
			err := s.db.Set(ctx, cmd.Key, cmd.Value)
			if err != nil {
				return nil, err
			}
			resp := redis.NewStatusCmd(ctx)
			resp.SetVal("OK")
			return resp, nil
		case CmdDelete:
			err := s.db.Delete(ctx, cmd.Key)
			if err != nil {
				return nil, err
			}
			resp := redis.NewStatusCmd(ctx)
			resp.SetVal("OK")
			return resp, nil
		default:
			return nil, fmt.Errorf("invalid cmd type: %v", cmd.OP)
		}
	}
	return nil, nil
}

// ApplyMemberChange implement raft.StateMachine
func (s *Server) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	return nil, errors.New("not supported")
}

// Snapshot implement raft.StateMachine
func (s *Server) Snapshot() (proto.Snapshot, error) {
	// return s.db.GetSnapshot(context.Background())
	return nil, errors.New("not supported")
}

// ApplySnapshot implement raft.StateMachine
func (s *Server) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	// s.db.RecoverFromSnapshot(context)
	return errors.New("not supported")
}

// HandleFatalEvent implement raft.StateMachine
func (s *Server) HandleFatalEvent(err *raft.FatalError) {
	log.Panic("raft fatal error: %v", err)
}

// HandleLeaderChange implement raft.StateMachine
func (s *Server) HandleLeaderChange(leader uint64) {
	log.Info("raft leader change to %v", leader)
	s.leader = leader
}

func (s *Server) StartCommit(ctx context.Context) (func(cmds ...*Command) redis.Cmder, redis.Cmder) {
	var commit = func(cmds ...*Command) redis.Cmder {
		cmd, err := s.process(ctx, cmds)
		if err != nil {
			return cmd
		}
		return cmd
	}
	if s.leader != s.nodeID {
		// s.replyNotLeader(w)
		return commit, redis.NewCmd(ctx, "not leader")
	}
	return commit, nil
}

func (s *Server) process(ctx context.Context, cmds []*Command) (redis.Cmder, error) {
	log.Debug("start process commands: %v", cmds)
	data, err := json.Marshal(cmds)
	if err != nil {
		log.Error("marshal raft command failed: %v", err)
		// w.WriteHeader(http.StatusInternalServerError)
		return nil, err
	}

	f := s.rs.Submit(DefaultClusterID, data)
	respCh, errCh := f.AsyncResponse()
	select {
	case resp := <-respCh:
		if cmd, ok := resp.(redis.Cmder); ok {
			return cmd, nil
		}
	case err := <-errCh:
		return nil, err
	case <-time.After(DefaultRequestTimeout):
		return nil, os.ErrDeadlineExceeded
	}
	return nil, nil
}

func (s *Server) getByReadIndex(ctx context.Context, key string) ([]byte, error) {
	// if log.GetFileLogger().IsEnableDebug() {
	// 	log.Debug("get %s by ReadIndex", key)
	// }

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
