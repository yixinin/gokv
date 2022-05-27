package codec

import (
	"bytes"
	"encoding/binary"
	"strconv"
)

type Encoder interface {
	Encode(s []byte, ex ...uint64) Value
}

type byesEncoder struct {
}

func (e byesEncoder) Encode(b []byte, ex ...uint64) Value {
	v := Value{
		e: 0,
	}
	var size = len(b)
	if len(ex) > 0 {
		v.e = ex[0]
	}
	var data []byte
	if size == 4 || size == 5 {
		switch string(b) {
		case "true", "True", "TRUE":
			data = []byte{1}
			v.t = BoolType
			v.b = true
		case "false", "False", "FALSE":
			data = []byte{0}
			v.t = BoolType
			v.b = true
		}
	}
	if v.t != BoolType {
		v.t = getValType(b)
		switch v.t {
		case FloatType:
			f, _ := strconv.ParseFloat(BytesToString(b), 64)
			bytesBuffer := bytes.NewBuffer(make([]byte, 0, NumberSize))
			binary.Write(bytesBuffer, binary.BigEndian, f)
			data = bytesBuffer.Bytes()
			v.f = f
		case IntType:
			i, _ := StringBytes2Int64(b)
			bytesBuffer := bytes.NewBuffer(make([]byte, 0, NumberSize))
			binary.Write(bytesBuffer, binary.BigEndian, i)
			data = bytesBuffer.Bytes()
			v.i = i
		default:
			data = b
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

func (e byesEncoder) EncodeInt(i int64, ex ...uint64) Value {
	v := Value{
		e: 0,
		t: IntType,
	}
	bytesBuffer := bytes.NewBuffer(make([]byte, 0, NumberSize))
	binary.Write(bytesBuffer, binary.BigEndian, i)
	data := bytesBuffer.Bytes()
	v.i = i

	v.data = make([]byte, HeaderSize+len(data))
	v.data[0] = v.t

	var eb = make([]byte, ExpireSize)
	binary.BigEndian.PutUint64(eb, v.e)
	copy(v.data[TypeSize:HeaderSize], eb)

	copy(v.data[HeaderSize:], data)
	return v
}
