package protocol

import (
	"encoding"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/tiglabs/raft/util/log"
	"github.com/yixinin/gokv/codec"
)

type writer interface {
	io.Writer
	io.ByteWriter
	// io.StringWriter
	WriteString(s string) (n int, err error)
}

type Writer struct {
	writer

	lenBuf []byte
	numBuf []byte
}

func NewWriter(wr writer) *Writer {
	return &Writer{
		writer: wr,

		lenBuf: make([]byte, 64),
		numBuf: make([]byte, 64),
	}
}

func (w *Writer) WriteCmd(cmd redis.Cmder) error {
	return w.string(cmd.String())
}

func (w *Writer) WriteArgs(args []interface{}) error {
	if err := w.WriteByte(ArrayReply); err != nil {
		return err
	}

	if err := w.writeLen(len(args)); err != nil {
		return err
	}

	for _, arg := range args {
		if err := w.WriteArg(arg); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) writeLen(n int) error {
	w.lenBuf = strconv.AppendUint(w.lenBuf[:0], uint64(n), 10)
	w.lenBuf = append(w.lenBuf, '\r', '\n')
	_, err := w.Write(w.lenBuf)
	return err
}

func (w *Writer) WriteArg(v interface{}) error {
	switch v := v.(type) {
	case nil:
		return w.string("")
	case string:
		return w.string(v)
	case []byte:
		return w.bytes(StringReply, v)
	case int:
		return w.int(int64(v))
	case int8:
		return w.int(int64(v))
	case int16:
		return w.int(int64(v))
	case int32:
		return w.int(int64(v))
	case int64:
		return w.int(v)
	case uint:
		return w.uint(uint64(v))
	case uint8:
		return w.uint(uint64(v))
	case uint16:
		return w.uint(uint64(v))
	case uint32:
		return w.uint(uint64(v))
	case uint64:
		return w.uint(v)
	case float32:
		return w.float(float64(v))
	case float64:
		return w.float(v)
	case bool:
		if v {
			return w.int(1)
		}
		return w.int(0)
	case time.Time:
		w.numBuf = v.AppendFormat(w.numBuf[:0], time.RFC3339Nano)
		return w.bytes(IntReply, w.numBuf)
	case time.Duration:
		return w.int(v.Nanoseconds())
	case encoding.BinaryMarshaler:
		b, err := v.MarshalBinary()
		if err != nil {
			return err
		}
		return w.bytes(StringReply, b)
	case net.IP:
		return w.bytes(StringReply, v)
	default:
		return fmt.Errorf(
			"redis: can't marshal %T (implement encoding.BinaryMarshaler)", v)
	}
}

func (w *Writer) WriteMessage(msg string) error {
	return w.bytes(StringReply, codec.StringToBytes(msg))
}

func (w *Writer) WriteWrongArgs(args []interface{}) error {
	msg := fmt.Sprintf("args[%v] error", args)
	return w.bytes(StringReply, codec.StringToBytes(msg))
}
func (w *Writer) WriteNotLeader(host string, port uint32) error {
	msg := fmt.Sprintf("leader %s:%d", host, port)
	return w.bytes(StringReply, codec.StringToBytes(msg))
}

func (w *Writer) bytes(t byte, b []byte) error {
	log.Debug("write %d->%s", t, b)
	if err := w.WriteByte(t); err != nil {
		return err
	}
	if t == StringReply || t == ArrayReply {
		if err := w.writeLen(len(b)); err != nil {
			return err
		}
	}

	if _, err := w.Write(b); err != nil {
		return err
	}

	return w.crlf()
}

func (w *Writer) string(s string) error {
	return w.bytes(StringReply, codec.StringToBytes(s))
}

func (w *Writer) uint(n uint64) error {
	w.numBuf = strconv.AppendUint(w.numBuf[:0], n, 10)
	return w.bytes(IntReply, w.numBuf)
}

func (w *Writer) int(n int64) error {
	w.numBuf = strconv.AppendInt(w.numBuf[:0], n, 10)
	return w.bytes(IntReply, w.numBuf)
}

func (w *Writer) float(f float64) error {
	w.numBuf = strconv.AppendFloat(w.numBuf[:0], f, 'f', -1, 64)
	return w.bytes(IntReply, w.numBuf)
}

func (w *Writer) crlf() error {
	if err := w.WriteByte('\r'); err != nil {
		return err
	}
	return w.WriteByte('\n')
}
