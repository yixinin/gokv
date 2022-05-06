package gokv

import "github.com/yixinin/gokv/storage"

type Server struct {
	*_hashImpl
	*_setImpl
}

func NewServer(db storage.Storage) *Server {
	return &Server{
		_hashImpl: &_hashImpl{_db: db},
		_setImpl:  &_setImpl{_db: db},
	}
}
