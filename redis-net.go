package gokv

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/tiglabs/raft/util/log"
	"github.com/yixinin/gokv/logger"
	"github.com/yixinin/gokv/redis/protocol"
	raft "go.etcd.io/etcd/raft/v3"
)

type Cmd struct {
	Addr net.Addr
	Cmd  []interface{}
	raft.Peer
}

type _netImpl struct {
	lis       net.Listener
	clients   map[string]net.Conn
	clientCmd chan Cmd
	s         *Server
}

func NewNetImpl(s *Server) *_netImpl {
	return &_netImpl{
		clients:   make(map[string]net.Conn),
		clientCmd: make(chan Cmd),
		s:         s,
	}
}

func (n *_netImpl) Close(ctx context.Context) {
	if n.lis != nil {
		n.lis.Close()
	}
}

func (n *_netImpl) listen(ctx context.Context, port uint16) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	logger.Info(ctx, "listen on", port)
	n.lis = lis

	go n.receive(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			conn, err := lis.Accept()
			if err != nil {
				return err
			}
			go n.addClient(ctx, conn)
		}
	}
}

func (n *_netImpl) addClient(ctx context.Context, conn net.Conn) {
	err := conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err != nil {
		logger.Error(ctx, err)
		return
	}
	n.clients[conn.RemoteAddr().String()] = conn
	rd := protocol.NewReader(conn)
	for {
		cmd, err := rd.ReadRequest(protocol.SliceParser)
		if os.IsTimeout(err) {
			continue
		}

		if err != nil {
			logger.Error(ctx, err)
			return
		}
		switch cmd := cmd.(type) {
		case []interface{}:
			n.clientCmd <- Cmd{
				Addr: conn.RemoteAddr(),
				Cmd:  cmd,
			}
		case string, interface{}:
			n.clientCmd <- Cmd{
				Addr: conn.RemoteAddr(),
				Cmd:  []interface{}{cmd},
			}
		default:
			logger.Info(ctx, "wrong cmd", cmd)
		}
	}
}

func (n *_netImpl) receive(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-n.clientCmd:
			n.handleCmd(context.Background(), cmd.Addr, cmd.Cmd)
		}
	}
}

func (n *_netImpl) handleCmd(ctx context.Context, addr net.Addr, args []interface{}) {
	if len(args) == 0 {
		logger.Info(ctx, "empty cmd")
		return
	}
	log.Debug("cmd args %v", args)
	cmd, ok := args[0].(string)
	if !ok {
		logger.Info(ctx, "wrong cmd", args)
		return
	}
	client, ok := n.clients[addr.String()]
	if !ok {
		logger.Info(ctx, "client disconnected", addr.String())
		return
	}
	w := protocol.NewWriter(client)
	switch cmd {
	case "set":
		commit, resp := n.s.StartCommit(ctx)
		if resp != nil {
			w.WriteString(resp.String())
			return
		}
		setCmd, ok := protocol.NewSetCmd(args)
		if !ok {
			logger.Info(ctx, "wrong cmd", args)
			return
		}
		cmd := n.s.Set(ctx, setCmd.Key, setCmd.Val, setCmd.Expire)
		resp = commit(cmd)
		fmt.Println(resp)
		w.WriteString(resp.String())
	case "get":
		commit, clusterResp := n.s.StartCommit(ctx)
		keyCmd, ok := protocol.NewKeyCmd(args)
		if !ok {
			logger.Info(ctx, "wrong cmd", args)
			return
		}
		cmd, resp := n.s.Get(ctx, keyCmd.Key)
		if resp != nil {
			w.WriteString(resp.String())
			return
		}
		if clusterResp == nil {
			// is leader, delete expired key
			resp = commit(cmd)
			w.WriteString(resp.String())
		}
	case "del":
		commit, resp := n.s.StartCommit(ctx)
		if resp != nil {
			w.WriteString(resp.String())
			return
		}
		keyCmd, ok := protocol.NewKeyCmd(args)
		if !ok {
			logger.Info(ctx, "wrong cmd", args)
			return
		}
		cmd := n.s.Delete(ctx, keyCmd.Key)
		resp = commit(cmd)
		w.WriteString(resp.String())
	case "ttl":
	case "expire":
	case "hset":
	case "hget":
	case "hgetall":
	case "incr":
	case "COMMAND":
		buf, _ := os.ReadFile("cache/cmd.txt")
		w.Write(buf)
	default:
		fmt.Println("unsurpport command", args)
	}
}
