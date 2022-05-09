package codec

import (
	"bytes"
	"encoding/binary"
	"strconv"
)

type Encoder interface {
	Encode(s string, ex ...uint64) Value
}

type byesEncoder struct {
}

func (e byesEncoder) Encode(s string, ex ...uint64) Value {
	v := Value{
		e: 0,
	}
	if len(ex) > 0 {
		v.e = ex[0]
	}

	var data []byte
	switch s {
	case "true", "True", "TRUE":
		data = []byte{1}
		v.t = BoolType
		v.b = true
	case "false", "False", "FALSE":
		data = []byte{0}
		v.t = BoolType
		v.b = true
	default:
		v.t = getValType(s)
		switch v.t {
		case FloatType:
			f, _ := strconv.ParseFloat(s, 64)
			bytesBuffer := bytes.NewBuffer(make([]byte, 0, NumberSize))
			binary.Write(bytesBuffer, binary.BigEndian, f)
			data = bytesBuffer.Bytes()
			v.f = f
		case IntType:
			i, _ := strconv.ParseInt(s, 10, 64)
			bytesBuffer := bytes.NewBuffer(make([]byte, 0, NumberSize))
			binary.Write(bytesBuffer, binary.BigEndian, i)
			data = bytesBuffer.Bytes()
			v.i = i
		default:
			data = []byte(s)
		}
	}
	v.data = make([]byte, HeaderSize+len(data))
	v.data[0] = v.t

	var eb = make([]byte, ExpireSize)
	binary.BigEndian.PutUint64(eb, v.e)
	copy(v.data[TypeSize:HeaderSize], eb)

	copy(v.data[HeaderSize:], data)
	return v
}
