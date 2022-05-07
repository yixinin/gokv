package gokv

import "errors"

var ErrNotfound = errors.New("not found")
var ErrKeyOPType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
var ErrValOpType = errors.New("WRONGTYPE VALUE")
