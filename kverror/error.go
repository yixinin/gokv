package kverror

import (
	"errors"
	"fmt"
	"strings"
)

var ErrNotFound = errors.New("not found")
var ErrNIL = errors.New("nil")
var ErrKeyOPType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
var ErrValOpType = errors.New("WRONGTYPE VALUE")
var ErrNotLeaderr = errors.New("not leader")

var ErrCommandArgs = errors.New("command args error")
var ErrCommandNotSupport = errors.New("command not support")
var ErrNotImpl = errors.New("not impl")

type KvError struct {
	Code     int      `json:"-"`
	Messages []string `json:"-"`
	Err      error    `json:"-"`
	Stack    string   `json:"-"`
}

func (e *KvError) Error() string {
	var sb = strings.Builder{}
	if e.Code != 0 {
		sb.WriteString(fmt.Sprintf("code:%d", e.Code))
	}
	if len(e.Messages) > 0 {
		if sb.Len() > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("msg:")
		sb.WriteString(strings.Join(e.Messages, ", "))
	}
	if e.Err != nil {
		if sb.Len() > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("err:%v", e.Err))
	}
	return sb.String()
}

func WrapError(err error, msg ...string) error {
	if err == nil {
		return err
	}
	e, ok := err.(*KvError)
	if !ok {
		e = &KvError{
			Err: err,
		}
		e.Stack = getStacks()
	}

	e.Messages = append(e.Messages, msg...)
	return e
}
func WrapCode(err error, code int) error {
	if err == nil {
		return &KvError{
			Code: code,
		}
	}
	e, ok := err.(*KvError)
	if !ok {
		e = &KvError{
			Err: err,
		}
		e.Stack = getStacks()
	}
	e.Code = code
	return e
}

func NewError(code int, msg string) error {
	e := &KvError{
		Code:     code,
		Messages: []string{msg},
	}
	e.Stack = getStacks()
	return e
}
