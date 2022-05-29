package kverror

import "errors"

var ErrNotFound = errors.New("not found")
var ErrKeyOPType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
var ErrValOpType = errors.New("WRONGTYPE VALUE")
var ErrNotLeaderr = errors.New("not leader")
