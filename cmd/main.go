package main

import (
	"context"
	"flag"
	"path"
	"strings"

	"github.com/yixinin/gokv"
	"github.com/yixinin/gokv/kvstore"
	"github.com/yixinin/gokv/kvstore/leveldb"
	"github.com/yixinin/gokv/kvstore/memdb"
	"go.etcd.io/etcd/raft/raftpb"
)

var db string
var dataPath string

func flags() {
	flag.StringVar(&db, "db", "mem", "leveldb or memdb")
	flag.Parse()
}

const (
	LEVELDB = "leveldb"
	MEMDB   = "memdb"
)

func main() {
	flags()

	var storage kvstore.Kvstore
	switch strings.ToLower(db) {
	case LEVELDB:
		var err error
		storage, err = leveldb.NewStorage(path.Join(dataPath, "leveldb"))
		if err != nil {
			panic(err)
		}
	case MEMDB:
		storage = memdb.NewStorage()
	}

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var s = gokv.NewServer(storage)
	go s.GC(ctx)

}

func newRaft() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	var kvs *gokv.Server
	getSnapshot := func(ctx context.Context) ([]byte, error) { return kvs.GetSnapshot(ctx) }
	commitC, errorC, snapshotterReady := gokv.NewRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)
	var db = memdb.NewStorage()
	kvs = gokv.NewServer(db, <-snapshotterReady, proposeC, commitC, errorC)

	kvs.Run(context.Background())
}
