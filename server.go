package gokv

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

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
	lis         net.Listener
	clients     map[string]*Client
	messageChan chan Message
	kv          *RaftKv
}

type Client struct {
	conn net.Conn
	rd   *protocol.Reader
	bw   *bufio.Writer
	wr   *protocol.Writer
}

func NewServer(kv *RaftKv) *Server {
	return &Server{
		clients:     make(map[string]*Client),
		messageChan: make(chan Message, 100),
		kv:          kv,
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
			logger.Errorf(ctx, "tcpReceive recovered from panic:%v, stacks:%s", r, debug.Stack())
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
				logger.Errorf(ctx, "set client conn timeout error:%v, this conn will disconnect", err)
				return
			}
			cmd, err := c.rd.ReadRequest(protocol.SliceParser)
			if os.IsTimeout(err) {
				continue loop
			}

			if err != nil {
				if err != io.EOF {
					logger.Errorf(ctx, "receive redis cmd error:%v, conn:%s will be disconnect", err, conn.RemoteAddr())
				}
				return
			}
			switch cmd := cmd.(type) {
			case []interface{}:
				n.messageChan <- Message{
					Addr: conn.RemoteAddr(),
					args: cmd,
				}
			case []byte, string, interface{}:
				n.messageChan <- Message{
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
		case cmd := <-n.messageChan:
			ctx := context.Background()
			// ctx = context.WithValue(context.Background(), trace.TraceKey, trace.GenTrace())
			err := n.handleCmd(ctx, cmd.Addr, cmd.args)
			if err != nil {
				logger.Errorf(ctx, "handleCmd error:%v", err)
			}
		}
	}
}

func (n *Server) handleCmd(ctx context.Context, addr net.Addr, args []interface{}) error {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf(ctx, "handleCmd recovered from panic:%v, stacks:%s", r, debug.Stack())
		}
	}()
	n.RLock()
	client, ok := n.clients[addr.String()]
	n.RUnlock()
	if !ok {
		logger.Info(ctx, "client disconnected", addr.String())
		return nil
	}
	base := protocol.Command(ctx, args)
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
		submit, ok := n.kv.StartSubmit(ctx)
		if !ok {
			return n.replyLeader(client.wr)
		}

		cmd := protocol.NewSetCmd(base)
		if cmd.Err != nil {
			return cmd.Write(client.wr)
		}
		ct := n.kv.Set(ctx, cmd)
		if ct != nil {
			cmd.OK, cmd.Err = submit(ct)
		}
		return cmd.Write(client.wr)
	case "get":
		cmd := protocol.NewGetCmd(base)
		if cmd.Err != nil {
			return cmd.Write(client.wr)
		}
		st := n.kv.Get(ctx, cmd)
		if st != nil && ok {
			n.kv.SubmitAsync(st)
		}
		return cmd.Write(client.wr)
	case "del":
		submit, ok := n.kv.StartSubmit(ctx)
		if !ok {
			return n.replyLeader(client.wr)
		}
		cmd := protocol.NewDelCmd(base)
		if cmd.Err != nil {
			return cmd.Write(client.wr)
		}
		st := n.kv.Delete(ctx, cmd.BaseCmd)
		if st != nil {
			cmd.OK, cmd.Err = submit(st)
		}

		return cmd.Write(client.wr)
	case "ttl":
		cmd := protocol.NewTTLCmd(base)
		if cmd.Err != nil {
			return n.replyLeader(client.wr)
		}
		st := n.kv.TTL(ctx, cmd)
		if st != nil && ok {
			n.kv.SubmitAsync(st)
		}
		return cmd.Write(client.wr)
	case "expire":
		submit, ok := n.kv.StartSubmit(ctx)
		if !ok {
			return n.replyLeader(client.wr)
		}
		cmd := protocol.NewExpirecmd(base)
		if cmd.Err != nil {
			cmd.Write(client.wr)
		}
		ct := n.kv.ExpireAt(ctx, cmd)
		if ct != nil {
			cmd.OK, cmd.Err = submit(ct)
		}
		return cmd.Write(client.wr)
	case "hset":
		fallthrough
	case "hget":
		fallthrough
	case "hgetall":
		fallthrough
	case "incrby":
		submit, ok := n.kv.StartSubmit(ctx)
		if !ok {
			return n.replyLeader(client.wr)
		}
		cmd := protocol.NewIncrByCmd(base)
		if cmd.Err != nil {
			return cmd.Write(client.wr)
		}
		ct := n.kv.Incr(ctx, cmd)
		if ct != nil {
			ok, err := submit(ct)
			if !ok {
				cmd.Val = 0
			}
			cmd.Err = err
		}
		return cmd.Write(client.wr)
	case "incr":
		submit, ok := n.kv.StartSubmit(ctx)
		if !ok {
			return n.replyLeader(client.wr)
		}
		cmd := protocol.NewIncrCmd(base)
		if cmd.Err != nil {
			return cmd.Write(client.wr)
		}
		ct := n.kv.Incr(ctx, cmd)
		if ct != nil {
			ok, err := submit(ct)
			if !ok {
				cmd.Val = 0
			}
			cmd.Err = err
		}
		return cmd.Write(client.wr)
	case "decrby":
		submit, ok := n.kv.StartSubmit(ctx)
		if !ok {
			return n.replyLeader(client.wr)
		}
		cmd := protocol.NewDecrByCmd(base)
		if cmd.Err != nil {
			return cmd.Write(client.wr)
		}
		ct := n.kv.Incr(ctx, cmd)
		if ct != nil {
			ok, err := submit(ct)
			if !ok {
				cmd.Val = 0
			}
			cmd.Err = err
		}
		return cmd.Write(client.wr)
	case "decr":
		submit, ok := n.kv.StartSubmit(ctx)
		if !ok {
			return n.replyLeader(client.wr)
		}
		cmd := protocol.NewDecrCmd(base)
		if cmd.Err != nil {
			return cmd.Write(client.wr)
		}
		ct := n.kv.Incr(ctx, cmd)
		if ct != nil {
			ok, err := submit(ct)
			if !ok {
				cmd.Val = 0
			}
			cmd.Err = err
		}
		return cmd.Write(client.wr)
	case "command":
		cmd := protocol.NewCommandsInfoCmd()
		return cmd.Write(client.wr)
	case "sentinel":
		cmd := protocol.NewSentinelCmd(args)
		leader := n.kv.getLeader()
		if leader != nil {
			cmd.MasterAddr[0] = leader.Host
			cmd.MasterAddr[1] = fmt.Sprint(leader.HTTPPort)
		}
		for i, node := range n.kv.cfg.ClusterCfg.Nodes {
			if node == leader {
				continue
			}
			var s = make([]string, 0, 4)
			s = append(s, "ip", node.Host)
			s = append(s, "port", fmt.Sprint(node.HTTPPort))
			cmd.SlaveAddrs[i] = s
		}
		return cmd.Write(client.wr)
	default:
		base.Err = kverror.ErrCommandNotSupport
		return base.Write(client.wr)
	}
}

func (s *Server) replyLeader(w *protocol.Writer) error {
	var leader = s.kv.getLeader()
	if leader != nil {
		return w.WriteNotLeader(leader.Host, leader.HTTPPort)
	}
	return nil
}
