package gokv

import (
	"context"
)

type Server struct {
	netImpl *_netImpl
}

func NewServer(kv *KvEngine) *Server {
	return &Server{
		netImpl: NewNetImpl(kv),
	}
}

func (s *Server) Run(ctx context.Context) error {
	err := s.netImpl.listen(ctx, 6479)
	if err != nil {
		return err
	}
	return nil
}
