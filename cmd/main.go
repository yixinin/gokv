package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/yixinin/gokv"
	"github.com/yixinin/gokv/kvstore/memdb"
	"github.com/yixinin/gokv/logger"
	"github.com/yixinin/gokv/redis/protocol"
	"go.etcd.io/etcd/raft/v3/raftpb"
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

func lis() {
	var lis, err = gokv.NewStoppableListener(":6378", make(<-chan struct{}))
	if err != nil {
		fmt.Println(err)
		return
	}
	for i := 0; i < 10; i++ {
		conn, err := lis.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		rd := protocol.NewReader(conn)
		wb := bufio.NewWriter(conn)
		w := protocol.NewWriter(wb)
		for {
			req, err := rd.ReadRequest(protocol.SliceParser)
			fmt.Println(req, err)
			var cmdsI, ok = req.([]interface{})
			if err != nil || len(cmdsI) == 0 || !ok {
				fmt.Println(err)
				break
			}
			switch cmd, _ := cmdsI[0].(string); cmd {
			case "ping":
				fmt.Println("write PONG")
				err := w.WriteStatus("PONG")
				err = wb.Flush()
				fmt.Println(err)
			case "set":
				fmt.Println("write OK")
				var ok = "OK"
				err := w.WriteStatus(ok)
				err = wb.Flush()
				fmt.Println(err)
			default:
				fmt.Println("unimpl cmd")
				w.WriteError("unimpl cmd: " + cmd)
				err = wb.Flush()
				return
			}
		}
	}

}

func main() {
	newRaft()
}

func newRaft() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	// kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	var kvs *gokv.KvEngine
	getSnapshot := func(ctx context.Context) ([]byte, error) { return kvs.GetSnapshot(ctx) }
	commitC, errorC, snapshotterReady := gokv.NewRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)

	kvs = gokv.NewKvEngine(memdb.NewStorage(), <-snapshotterReady, proposeC, commitC, errorC)
	server := gokv.NewServer(kvs)
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	err := server.Run(ctx)
	if err != nil {
		logger.Error(ctx, err)
	}
}
