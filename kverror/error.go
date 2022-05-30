package kverror

import "errors"

var ErrNotFound = errors.New("not found")
var ErrNIL = errors.New("nil")
var ErrKeyOPType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
var ErrValOpType = errors.New("WRONGTYPE VALUE")
var ErrNotLeaderr = errors.New("not leader")

var ErrCommandArgs = errors.New("command args error")
var ErrCommandNotSupport = errors.New("command not support")

type KvError struct {
	Message string
}

func (e *KvError) Error() string {
	return e.Message
}

func NewKvError(msg string) error {
	return &KvError{
		Message: msg,
	}
}
