package gokv

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

func (n *_netImpl) listenCluster(ctx context.Context, port uint16) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	n.clusterLis = lis

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			conn, err := lis.Accept()
			if err != nil {
				return err
			}
			go n.addCluster(ctx, conn)
		}
	}
}
func (n *_netImpl) addCluster(ctx context.Context, conn net.Conn) {
	err := conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err != nil {
		return
	}

	n.clusters[conn.RemoteAddr()] = conn
	for {
		var buf = make([]byte, 1024)
		nn, err := conn.Read(buf)
		if errors.Is(err, ctx.Err()) {
			continue
		}
		if err != nil {
			return
		}
		n.clusterCmd <- Cmd{
			Addr: conn.RemoteAddr(),
			Cmd:  buf[:nn],
		}
	}
}
