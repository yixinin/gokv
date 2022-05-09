package codec

import (
	"bytes"
	"encoding/binary"
)

type Decoder interface {
	Decode(data []byte) Value
}

type bytesDecoder struct {
}

func (d bytesDecoder) Decode(data []byte) Value {
	var v = Value{}
	var size = len(data)
	if size < 2+8 {
		return Value{}
	}
	v.raw = data
	switch data[0] {
	case BoolType:
		v.b = data[9] == 1
	case IntType:
		var i int64
		binary.Read(bytes.NewBuffer(data[9:]), binary.BigEndian, &i)
		v.i = i
	case FloatType:
		var f float64
		binary.Read(bytes.NewBuffer(data[9:]), binary.BigEndian, &f)
		v.f = f
	}
	v.e = binary.BigEndian.Uint64(data[1:9])
	return v
}
