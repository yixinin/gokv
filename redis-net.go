package gokv

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
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

type _netImpl struct {
	lis       net.Listener
	clients   map[string]*Client
	clientCmd chan Message
	s         *Server
}

type Client struct {
	conn net.Conn
	rd   *protocol.Reader
	bw   *bufio.Writer
	wr   *protocol.Writer
}

func NewNetImpl(s *Server) *_netImpl {
	return &_netImpl{
		clients:   make(map[string]*Client),
		clientCmd: make(chan Message),
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
			go n.tcpReceive(ctx, conn)
		}
	}
}

func (n *_netImpl) tcpReceive(ctx context.Context, conn net.Conn) {
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

func (n *_netImpl) receive(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-n.clientCmd:
			go n.handleCmd(context.Background(), cmd.Addr, cmd.args)
		}
	}
}

func (n *_netImpl) handleCmd(ctx context.Context, addr net.Addr, args []interface{}) error {
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
			formatedArgs = append(formatedArgs, fmt.Sprintf("%s", arg))
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
	switch codec.BytesToString(cmd) {
	case "set":
		commit, leader := n.s.StartCommit(ctx)
		if leader != nil {
			return client.wr.WriteNotLeader(leader.Host, leader.HTTPPort)
		}
		cmd, ok := protocol.NewSetCmd(args)
		if !ok {
			return client.wr.WriteWrongArgs(args)
		}
		ct := n.s.Set(ctx, cmd)
		if ok, message := commit(ct); ok != "" {
			return client.wr.WriteMessage(ok)
		} else {
			return client.wr.WriteMessage(message)
		}
	case "get":
		commit, leader := n.s.StartCommit(ctx)
		cmd, ok := protocol.NewGetCmd(args)
		if !ok {
			return client.wr.WriteWrongArgs(args)
		}
		ct := n.s.Get(ctx, cmd)
		if ct != nil && leader == nil {
			go commit(ct)
		}
		cmd.Write(client.wr)
	case "del":
		commit, leader := n.s.StartCommit(ctx)
		if leader != nil {
			return client.wr.WriteNotLeader(leader.Host, leader.HTTPPort)
		}
		keyCmd, ok := protocol.NewDelCmd(args)
		if !ok {
			return client.wr.WriteWrongArgs(args)
		}
		cmd := n.s.Delete(ctx, keyCmd.BaseCmd)
		if ok, message := commit(cmd); ok != "" {
			return client.wr.WriteMessage(ok)
		} else {
			return client.wr.WriteMessage(message)
		}
	case "ttl":
		commit, leader := n.s.StartCommit(ctx)
		cmd, ok := protocol.NewTTLCmd(args)
		if !ok {
			return client.wr.WriteWrongArgs(args)
		}
		ct := n.s.TTL(ctx, cmd)
		if commit != nil && leader == nil {
			go commit(ct)
		}
		return cmd.Write(client.wr)
	case "expire":
		commit, leader := n.s.StartCommit(ctx)
		if leader != nil {
			return client.wr.WriteNotLeader(leader.Host, leader.HTTPPort)
		}
		cmd, ok := protocol.NewExpirecmd(args)
		if !ok {
			return client.wr.WriteWrongArgs(args)
		}
		ct := n.s.ExpireAt(ctx, cmd)
		if ct != nil {
			ok, msg := commit(ct)
			if ok != "" {
				cmd.Message = ok
			}
			if msg != "" {
				cmd.Message = msg
			}
		}
		return cmd.Write(client.wr)
	case "hset":
	case "hget":
	case "hgetall":
	case "incrby":
		commit, leader := n.s.StartCommit(ctx)
		if leader != nil {
			return client.wr.WriteNotLeader(leader.Host, leader.HTTPPort)
		}
		cmd, ok := protocol.NewIncrByCmd(args)
		if !ok {
			return client.wr.WriteWrongArgs(args)
		}
		ct := n.s.Incr(ctx, cmd)
		if ct != nil {
			ok, msg := commit(ct)
			if ok != SetOK {
				cmd.Val = 0
				cmd.Message = ok
			}
			if msg != "" {
				cmd.Message = msg
			}
		}
		cmd.Write(client.wr)
	case "incr":
		commit, leader := n.s.StartCommit(ctx)
		if leader != nil {
			return client.wr.WriteNotLeader(leader.Host, leader.HTTPPort)
		}
		cmd, ok := protocol.NewIncrCmd(args)
		if !ok {
			return client.wr.WriteWrongArgs(args)
		}
		ct := n.s.Incr(ctx, cmd)
		if ct != nil {
			ok, e := commit(ct)
			if ok != SetOK {
				cmd.Val = 0
				cmd.Message = ok
			}
			if e != "" {
				cmd.Message = e
			}
		}
		return cmd.Write(client.wr)
	case "command":
		log.Debug("commands none")
		return protocol.NewCommandsInfoCmd().Write(client.wr)
	default:
		return client.wr.WriteWrongArgs(args)
	}
	return nil
}
