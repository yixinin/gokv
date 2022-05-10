package gokv

import (
	"context"

	"github.com/yixinin/gokv/impls/snap"
)

type Server struct {
	kv         KvEngine
	snapShoter *snap.Snapshotter
	proposeC   chan string
	commitC    <-chan *commit
	errorC     <-chan error
}

func NewServer(kv KvEngine) *Server {
	return &Server{
		kv: kv,
	}
}

func (s *Server) GetSnapshot(ctx context.Context) ([]byte, error) {
	return s.kv.GetSnapshot(ctx)
}

func (s *Server) Run(ctx context.Context) {

}

func (s *Server) handleHash(args []string) {

}

func (s *Server) WithHash() *_hashImpl {
	return &_hashImpl{
		_db: &CmdContainer{
			cmds: make(KvCmds, 0, 1),
			db:   s.kv._kv,
		},
	}
}
