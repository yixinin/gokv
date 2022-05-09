package gokv

import "github.com/yixinin/gokv/kvstore"

type Server struct {
	*_hashImpl
	*_setImpl
	*_ttlImpl
	*_numImpl
}

func NewServer(db kvstore.Kvstore) *Server {
	return &Server{
		_hashImpl: &_hashImpl{_db: db},
		_setImpl:  &_setImpl{_db: db},
		_ttlImpl:  &_ttlImpl{_db: db},
		_numImpl:  &_numImpl{_db: db},
	}
}
