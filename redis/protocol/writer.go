package protocol

import (
	"fmt"
	"io"
	"strconv"

	"github.com/yixinin/gokv/codec"
	"github.com/yixinin/gokv/kverror"
)

var NIL = []byte("-1")
var PONG = []byte("PONG")

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

func (w *Writer) writeLen(n int) error {
	w.lenBuf = strconv.AppendUint(w.lenBuf[:0], uint64(n), 10)
	w.lenBuf = append(w.lenBuf, '\r', '\n')
	_, err := w.Write(w.lenBuf)
	return err
}

func (w *Writer) writeError(err error) error {
	switch err {
	case kverror.ErrNotFound:
		return w.bytes(ErrorReply, NIL)
	}
	return w.bytes(ErrorReply, codec.StringToBytes(err.Error()))
}

func (w *Writer) writeArray(msg ...string) error {
	w.WriteByte(ArrayReply)
	w.writeLen(len(msg))
	for i := range msg {
		w.WriteByte(StringReply)
		w.writeLen(len(msg[i]))
		w.WriteString(msg[i])
		w.crlf()
	}

	return nil
}

func (w *Writer) Pong() error {
	return w.bytes(StatusReply, PONG)
}

func (w *Writer) WriteWrongArgs(args []interface{}) error {
	msg := fmt.Sprintf("args[%v] error", args)
	return w.bytes(ErrorReply, codec.StringToBytes(msg))
}
func (w *Writer) WriteNotLeader(host string, port uint32) error {
	w.WriteByte(ErrorReply)
	msg := fmt.Sprintf("leader %s:%d", host, port)
	return w.bytes(StringReply, codec.StringToBytes(msg))
}
func (w *Writer) WriteClose() error {
	_, err := w.Write([]byte("EOF"))
	return err
}

func (w *Writer) bytes(t byte, b []byte) error {
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
