package protocol

import (
	"bufio"
	"fmt"
	"io"

	"github.com/yixinin/gokv/codec"
)

const (
	ErrorReply  = '-'
	StatusReply = '+'
	IntReply    = ':'
	StringReply = '$'
	ArrayReply  = '*'
)

//------------------------------------------------------------------------------

const Nil = RedisError("redis: nil") // nolint:errname

type RedisError string

func (e RedisError) Error() string { return string(e) }

func (RedisError) RedisError() {}

//------------------------------------------------------------------------------

type MultiBulkParse func(*Reader, int64) (interface{}, error)

type Reader struct {
	rd   *bufio.Reader
	_buf []byte
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{
		rd:   bufio.NewReader(rd),
		_buf: make([]byte, 64),
	}
}

func (r *Reader) Buffered() int {
	return r.rd.Buffered()
}

func (r *Reader) Peek(n int) ([]byte, error) {
	return r.rd.Peek(n)
}

func (r *Reader) Reset(rd io.Reader) {
	r.rd.Reset(rd)
}

func (r *Reader) ReadLine() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	return line, nil
}

// readLine that returns an error if:
//   - there is a pending read error;
//   - or line does not end with \r\n.
func (r *Reader) readLine() ([]byte, error) {
	b, err := r.rd.ReadSlice('\n')
	if err != nil {
		if err != bufio.ErrBufferFull {
			return nil, err
		}

		full := make([]byte, len(b))
		copy(full, b)

		b, err = r.rd.ReadBytes('\n')
		if err != nil {
			return nil, err
		}

		full = append(full, b...) //nolint:makezero
		b = full
	}
	if len(b) <= 2 || b[len(b)-1] != '\n' || b[len(b)-2] != '\r' {
		return nil, fmt.Errorf("redis: invalid reply: %q", b)
	}
	return b[:len(b)-2], nil
}

func (r *Reader) ReadRequest(m MultiBulkParse) (interface{}, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case ErrorReply:
		return nil, ParseErrorReply(line)
	case StatusReply:
		return codec.BytesToString(line[1:]), nil
	case IntReply:
		return codec.ParseInt(line[1:], 10, 64)
	case StringReply:
		return r.readStringReply(line)
	case ArrayReply:
		n, err := parseArrayLen(line)
		if err != nil {
			return nil, err
		}
		if m == nil {
			err := fmt.Errorf("redis: got %.100q, but multi bulk parser is nil", line)
			return nil, err
		}
		return m(r, n)
	}
	return nil, fmt.Errorf("redis: can't parse %.100q", line)
}

// sliceParser implements proto.MultiBulkParse.
func SliceParser(rd *Reader, n int64) (interface{}, error) {
	vals := make([]interface{}, n)
	for i := 0; i < len(vals); i++ {
		v, err := rd.ReadRequest(SliceParser)
		if err != nil {
			if err == Nil {
				vals[i] = nil
				continue
			}
			if err, ok := err.(RedisError); ok {
				vals[i] = err
				continue
			}
			return nil, err
		}
		vals[i] = v
	}
	return vals, nil
}

func (r *Reader) readStringReply(line []byte) ([]byte, error) {
	replyLen, err := codec.Atoi(line[1:])
	if err != nil {
		return nil, err
	}

	b := make([]byte, replyLen+2)
	_, err = io.ReadFull(r.rd, b)
	if err != nil {
		return nil, err
	}

	return b[:replyLen], nil
}

func ParseErrorReply(line []byte) error {
	return RedisError(string(line[1:]))
}

func parseArrayLen(line []byte) (int64, error) {
	return codec.ParseInt(line[1:], 10, 64)
}
