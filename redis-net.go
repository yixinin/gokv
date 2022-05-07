package gokv

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	raft "go.etcd.io/etcd/raft/v3"
)

type Cmd struct {
	Addr net.Addr
	Cmd  []byte
	raft.Peer
}

type _netImpl struct {
	clusterLis net.Listener
	clusters   map[net.Addr]net.Conn
	clusterCmd chan Cmd

	lis       net.Listener
	clients   map[net.Addr]net.Conn
	clientCmd chan Cmd
}

func (n *_netImpl) Close(ctx context.Context) {
	if n.lis != nil {
		n.lis.Close()
	}
	if n.clusterLis != nil {
		n.clusterLis.Close()
	}
}

func (n *_netImpl) listen(ctx context.Context, port uint16) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	n.lis = lis

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
		return
	}
	n.clients[conn.RemoteAddr()] = conn
	for {
		var buf = make([]byte, 1024)
		nn, err := conn.Read(buf)
		if errors.Is(err, ctx.Err()) {
			continue
		}
		if err != nil {
			return
		}
		n.clientCmd <- Cmd{
			Addr: conn.RemoteAddr(),
			Cmd:  buf[:nn],
		}
	}
}

func (n *_netImpl) handleCmd(ctx context.Context, addr net.Addr, cmd []byte) {

}
