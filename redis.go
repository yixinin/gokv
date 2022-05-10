package gokv

import (
	"context"

	"github.com/yixinin/gokv/impls/snap"
	"github.com/yixinin/gokv/kvstore"
)

type Server struct {
	*_hashImpl
	*_setImpl
	*_ttlImpl
	*_numImpl

	db kvstore.Kvstore

	snapShoter *snap.Snapshotter
	proposeC   chan string
	commitC    <-chan *commit
	errorC     <-chan error
}

func NewServer(db kvstore.Kvstore) *Server {
	return &Server{
		_hashImpl: &_hashImpl{_db: db},
		_setImpl:  &_setImpl{_db: db},
		_ttlImpl:  &_ttlImpl{_db: db},
		_numImpl:  &_numImpl{_db: db},
		db:        db,
	}
}

func (s *Server) GetSnapshot(ctx context.Context) ([]byte, error) {
	return s.db.GetSnapshot(ctx)
}

func (s *Server) Run(ctx context.Context) {

}
