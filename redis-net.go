package gokv

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

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

	engine *KvEngine
}

func NewNetImpl(engine *KvEngine) *_netImpl {
	return &_netImpl{
		clients:   make(map[string]net.Conn),
		clientCmd: make(chan Cmd),
		engine:    engine,
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
		base, commit := n.engine.BaseCmd()
		defer commit()
		setCmd, ok := protocol.NewSetCmd(args)
		if !ok {
			logger.Info(ctx, "wrong cmd", args)
			return
		}
		err := base.Set(ctx, setCmd.Key, setCmd.Val, setCmd.Expire)
		if err != nil {
			logger.Error(ctx, err)
		}
	case "get":
		base, commit := n.engine.BaseCmd()
		defer commit()
		keyCmd, ok := protocol.NewKeyCmd(args)
		if !ok {
			logger.Info(ctx, "wrong cmd", args)
			return
		}
		val, err := base.Get(ctx, keyCmd.Key)
		if err != nil {
			logger.Error(ctx, err)
			return
		}

		err = w.WriteString(val)
		if err != nil {
			logger.Error(ctx, err)
		}
	case "del":
		base, commit := n.engine.BaseCmd()
		defer commit()
		keyCmd, ok := protocol.NewKeyCmd(args)
		if !ok {
			logger.Info(ctx, "wrong cmd", args)
			return
		}
		err := base.Delete(ctx, keyCmd.Key)
		if err != nil {
			logger.Error(ctx, err)
		}
	case "ttl":
		base, commit := n.engine.TTLCmd()
		defer commit()
		ttlCmd, ok := protocol.NewKeyCmd(args)
		if !ok {
			logger.Info(ctx, "wrong cmd", args)
			return
		}
		ttl := base.TTL(ctx, ttlCmd.Key)
		err := w.WriteArg(ttl)
		if err != nil {
			logger.Error(ctx, err)
		}
	case "expire":
		base, commit := n.engine.TTLCmd()
		defer commit()
		expCmd, ok := protocol.NewExpirecmd(args)
		if !ok {
			log.Println("wrong cmd", args)
			return
		}
		ttl := base.TTL(ctx, expCmd.Key)
		err := w.WriteArg(ttl)
		if err != nil {
			logger.Error(ctx, err)
		}
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
