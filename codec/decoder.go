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
	if size < HeaderSize {
		return Value{}
	}
	v.data = data
	switch data[0] {
	case BoolType:
		v.b = data[HeaderSize] == 1
		v.t = BoolType
	case IntType:
		var i int64
		binary.Read(bytes.NewBuffer(data[HeaderSize:]), binary.BigEndian, &i)
		v.i = i
		v.t = IntType
	case FloatType:
		var f float64
		binary.Read(bytes.NewBuffer(data[HeaderSize:]), binary.BigEndian, &f)
		v.f = f
		v.t = FloatType
	default:
		v.t = StrType
	}
	v.e = binary.BigEndian.Uint64(data[TypeSize:HeaderSize])
	return v
}
