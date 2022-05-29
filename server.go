package gokv

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/tiglabs/raft/util/log"
	"github.com/yixinin/gokv/codec"
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

	n.clients[conn.RemoteAddr().String()] = c
	defer c.conn.Close()
	defer delete(n.clients, conn.RemoteAddr().String())
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
	client, ok := n.clients[addr.String()]
	if !ok {
		logger.Info(ctx, "client disconnected", addr.String())
		return nil
	}
	if len(args) == 0 {
		return client.wr.WriteWrongArgs(args)
	}
	var formatedArgs = make([]string, 0, len(args))
	for i := range args {
		switch arg := args[i].(type) {
		case []byte:
			formatedArgs = append(formatedArgs, string(arg))
		default:
			formatedArgs = append(formatedArgs, fmt.Sprint(arg))
		}
	}
	log.Debug("cmd %v", formatedArgs)
	cmd, ok := args[0].([]byte)
	if !ok {
		return client.wr.WriteWrongArgs(args)
	}
	defer client.bw.Flush()
	switch strings.ToLower(codec.BytesToString(cmd)) {
	case "set":
		commit, leader := n.kv.StartCommit(ctx)
		if leader != nil {
			return client.wr.WriteNotLeader(leader.Host, leader.HTTPPort)
		}
		cmd, ok := protocol.NewSetCmd(args)
		if !ok {
			return client.wr.WriteWrongArgs(args)
		}
		ct := n.kv.Set(ctx, cmd)
		cmd.OK, cmd.Err = commit(ct)
		return cmd.Write(client.wr)
	case "get":
		commit, leader := n.kv.StartCommit(ctx)
		cmd, ok := protocol.NewGetCmd(args)
		if !ok {
			return client.wr.WriteWrongArgs(args)
		}
		ct := n.kv.Get(ctx, cmd)
		if ct != nil && leader == nil {
			go commit(ct)
		}
		cmd.Write(client.wr)
	case "del":
		commit, leader := n.kv.StartCommit(ctx)
		if leader != nil {
			return client.wr.WriteNotLeader(leader.Host, leader.HTTPPort)
		}
		keyCmd, ok := protocol.NewDelCmd(args)
		if !ok {
			return client.wr.WriteWrongArgs(args)
		}
		ct := n.kv.Delete(ctx, keyCmd.BaseCmd)
		if ct != nil {
			keyCmd.OK, keyCmd.Err = commit(ct)
		}

		return keyCmd.Write(client.wr)
	case "ttl":
		commit, leader := n.kv.StartCommit(ctx)
		cmd, ok := protocol.NewTTLCmd(args)
		if !ok {
			return client.wr.WriteWrongArgs(args)
		}
		ct := n.kv.TTL(ctx, cmd)
		if commit != nil && leader == nil {
			go commit(ct)
		}
		return cmd.Write(client.wr)
	case "expire":
		commit, leader := n.kv.StartCommit(ctx)
		if leader != nil {
			return client.wr.WriteNotLeader(leader.Host, leader.HTTPPort)
		}
		cmd, ok := protocol.NewExpirecmd(args)
		if !ok {
			return client.wr.WriteWrongArgs(args)
		}
		ct := n.kv.ExpireAt(ctx, cmd)
		if ct != nil {
			cmd.OK, cmd.Err = commit(ct)
		}
		return cmd.Write(client.wr)
	case "hset":
	case "hget":
	case "hgetall":
	case "incrby":
		commit, leader := n.kv.StartCommit(ctx)
		if leader != nil {
			return client.wr.WriteNotLeader(leader.Host, leader.HTTPPort)
		}
		cmd, ok := protocol.NewIncrByCmd(args)
		if !ok {
			return client.wr.WriteWrongArgs(args)
		}
		ct := n.kv.Incr(ctx, cmd)
		if ct != nil {
			ok, err := commit(ct)
			if !ok {
				cmd.Val = 0
			}
			cmd.Err = err
		}
		cmd.Write(client.wr)
	case "incr":
		commit, leader := n.kv.StartCommit(ctx)
		if leader != nil {
			return client.wr.WriteNotLeader(leader.Host, leader.HTTPPort)
		}
		cmd, ok := protocol.NewIncrCmd(args)
		if !ok {
			return client.wr.WriteWrongArgs(args)
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
		log.Debug("commands none")
		return protocol.NewCommandsInfoCmd().Write(client.wr)
	case "ping":
		return client.wr.Pong()
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
		return client.wr.WriteWrongArgs(args)
	}
	return nil
}
