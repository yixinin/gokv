package gokv

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/tiglabs/raft/util/log"
	"github.com/yixinin/gokv/codec"
	"github.com/yixinin/gokv/kverror"
	"github.com/yixinin/gokv/logger"
	"github.com/yixinin/gokv/redis/protocol"
)

const (
	SetOK = "OK"
)

type Message struct {
	Addr net.Addr
	args []interface{}
}

type Server struct {
	sync.RWMutex
	lis       net.Listener
	clients   map[string]*Client
	clientCmd chan Message
	kv        *RaftKv
}

type Client struct {
	conn net.Conn
	rd   *protocol.Reader
	bw   *bufio.Writer
	wr   *protocol.Writer
}

func NewServer(kv *RaftKv) *Server {
	return &Server{
		clients:   make(map[string]*Client),
		clientCmd: make(chan Message),
		kv:        kv,
	}
}

func (n *Server) Close(ctx context.Context) {
	if n.lis != nil {
		n.lis.Close()
	}
}

func (n *Server) Run(ctx context.Context, port uint32) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	logger.Info(ctx, "listen on ", port)
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
			go n.readAsync(ctx, conn)
		}
	}
}

func (n *Server) readAsync(ctx context.Context, conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("tcpReceive recovered from panic:%v, stacks:%s", r, debug.Stack())
		}
	}()
	// err := conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	// if err != nil {
	// 	logger.Error(ctx, err)
	// 	return
	// }
	c := &Client{
		conn: conn,
		rd:   protocol.NewReader(conn),
		bw:   bufio.NewWriter(conn),
	}
	c.wr = protocol.NewWriter(c.bw)
	n.Lock()
	n.clients[conn.RemoteAddr().String()] = c
	n.Unlock()
	defer c.conn.Close()
	defer func() {
		n.Lock()
		delete(n.clients, conn.RemoteAddr().String())
		n.Unlock()
	}()
loop:
	for {
		select {
		case <-ctx.Done():
			c.wr.WriteClose()
			return
		default:
			err := c.conn.SetReadDeadline(time.Now().Add(time.Second))
			if err != nil {
				log.Error("set client conn timeout error:%v, this conn will disconnect", err)
				return
			}
			cmd, err := c.rd.ReadRequest(protocol.SliceParser)
			if os.IsTimeout(err) {
				continue loop
			}

			if err != nil {
				log.Error("receive redis cmd error:%v, this conn will disconnect", err)
				return
			}
			switch cmd := cmd.(type) {
			case []interface{}:
				n.clientCmd <- Message{
					Addr: conn.RemoteAddr(),
					args: cmd,
				}
			case []byte, string, interface{}:
				n.clientCmd <- Message{
					Addr: conn.RemoteAddr(),
					args: []interface{}{cmd},
				}
			default:
				logger.Info(ctx, "wrong cmd", cmd)
			}
		}
	}
}

func (n *Server) receive(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-n.clientCmd:
			go n.handleCmd(context.Background(), cmd.Addr, cmd.args)
		}
	}
}

func (n *Server) handleCmd(ctx context.Context, addr net.Addr, args []interface{}) error {
	defer func() {
		if r := recover(); r != nil {
			log.Error("handleCmd recovered from panic:%v, stacks:%s", r, debug.Stack())
		}
	}()
	n.RLock()
	client, ok := n.clients[addr.String()]
	n.RUnlock()
	if !ok {
		logger.Info(ctx, "client disconnected", addr.String())
		return nil
	}
	base := protocol.Command(args)
	if base.Err != nil {
		return client.wr.WriteWrongArgs(args)
	}
	cmd, ok := args[0].([]byte)
	if !ok {
		return client.wr.WriteWrongArgs(args)
	}
	defer client.bw.Flush()

	switch strings.ToLower(codec.BytesToString(cmd)) {
	case "ping":
		cmd := &protocol.PingCommand{}
		return cmd.Write(client.wr)
	case "set":
		commit, ok := n.kv.StartCommit(ctx)
		if !ok {
			return n.replyLeader(client.wr)
		}

		cmd := protocol.NewSetCmd(base)
		if cmd.Err != nil {
			return cmd.Write(client.wr)
		}
		ct := n.kv.Set(ctx, cmd)
		cmd.OK, cmd.Err = commit(ct)
		return cmd.Write(client.wr)
	case "get":
		commit, ok := n.kv.StartCommit(ctx)
		cmd := protocol.NewGetCmd(base)
		if cmd.Err != nil {
			return cmd.Write(client.wr)
		}
		ct := n.kv.Get(ctx, cmd)
		if ct != nil && ok {
			go commit(ct)
		}
		return cmd.Write(client.wr)
	case "del":
		commit, ok := n.kv.StartCommit(ctx)
		if !ok {
			return n.replyLeader(client.wr)
		}
		cmd := protocol.NewDelCmd(base)
		if cmd.Err != nil {
			return cmd.Write(client.wr)
		}
		ct := n.kv.Delete(ctx, cmd.BaseCmd)
		if ct != nil {
			cmd.OK, cmd.Err = commit(ct)
		}

		return cmd.Write(client.wr)
	case "ttl":
		commit, ok := n.kv.StartCommit(ctx)
		cmd := protocol.NewTTLCmd(base)
		if cmd.Err != nil {
			return n.replyLeader(client.wr)
		}
		ct := n.kv.TTL(ctx, cmd)
		if commit != nil && ok {
			go commit(ct)
		}
		return cmd.Write(client.wr)
	case "expire":
		commit, ok := n.kv.StartCommit(ctx)
		if !ok {
			return n.replyLeader(client.wr)
		}
		cmd := protocol.NewExpirecmd(base)
		if cmd.Err != nil {
			cmd.Write(client.wr)
		}
		ct := n.kv.ExpireAt(ctx, cmd)
		if ct != nil {
			cmd.OK, cmd.Err = commit(ct)
		}
		return cmd.Write(client.wr)
	case "hset":
		fallthrough
	case "hget":
		fallthrough
	case "hgetall":
		fallthrough
	case "incrby":
		commit, ok := n.kv.StartCommit(ctx)
		if !ok {
			return n.replyLeader(client.wr)
		}
		cmd := protocol.NewIncrByCmd(base)
		if cmd.Err != nil {
			return cmd.Write(client.wr)
		}
		ct := n.kv.Incr(ctx, cmd)
		if ct != nil {
			ok, err := commit(ct)
			if !ok {
				cmd.Val = 0
			}
			cmd.Err = err
		}
		return cmd.Write(client.wr)
	case "incr":
		commit, ok := n.kv.StartCommit(ctx)
		if !ok {
			return n.replyLeader(client.wr)
		}
		cmd := protocol.NewIncrCmd(base)
		if cmd.Err != nil {
			return cmd.Write(client.wr)
		}
		ct := n.kv.Incr(ctx, cmd)
		if ct != nil {
			ok, err := commit(ct)
			if !ok {
				cmd.Val = 0
			}
			cmd.Err = err
		}
		return cmd.Write(client.wr)
	case "command":
		_, ok := n.kv.StartCommit(ctx)
		cmd := protocol.NewCommandsInfoCmd(ok)
		return cmd.Write(client.wr)
	case "sentinel":
		cmd := protocol.NewSentinelCmd(args)
		leader := n.kv.getLeader()
		if leader != nil {
			cmd.MasterAddr[0] = leader.Host
			cmd.MasterAddr[1] = fmt.Sprint(leader.HTTPPort)
		}
		for _, node := range n.kv.cfg.ClusterCfg.Nodes {
			if node == leader {
				continue
			}
			cmd.SlaveAddrs = append(cmd.SlaveAddrs, []string{"ip", node.Host})
			cmd.SlaveAddrs = append(cmd.SlaveAddrs, []string{"port", fmt.Sprint(node.HTTPPort)})
		}
		return cmd.Write(client.wr)
	default:
		base.Err = kverror.ErrCommandNotSupport
		return base.Write(client.wr)
	}
	return nil
}

func (s *Server) replyLeader(w *protocol.Writer) error {
	var leader = s.kv.getLeader()
	if leader != nil {
		return w.WriteNotLeader(leader.Host, leader.HTTPPort)
	}
	return nil
}
