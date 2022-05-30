package codec

import (
	"encoding/binary"
	"strconv"
)

type Value struct {
	t uint8

	b bool
	i int64
	f float64

	data []byte

	e uint64
}

func (v *Value) CopyFrom(nv Value) {
	v.b = nv.b
	v.e = nv.e
	v.t = nv.t
	v.f = nv.f
	v.i = nv.i
	v.data = nv.data
}

func (v *Value) SetExpireAt(ex uint64) {
	v.e = ex
	eb := make([]byte, ExpireSize)
	binary.BigEndian.PutUint64(eb, ex)
	copy(v.data[1:HeaderSize], eb)

}

func (v *Value) Set(val []byte) {
	nv := Encode(val, v.ExpireAt())
	v.CopyFrom(nv)
}

func (v *Value) SetInt(val int64) {
	v.t = IntType
	v.SetBytes(Int642Bytes(val))
}

func (v *Value) SetFloat(f float64) {
	v.t = FloatType
	v.SetBytes(Float2Bytes(f))
}

func (v *Value) SetBool(b bool) {
	bs := make([]byte, 1)
	if b {
		bs[0] = 1
	}
	v.t = BoolType
	v.SetBytes(bs)
}
func (v *Value) SetBytes(bs []byte) {
	if len(v.data)-HeaderSize != len(bs) {
		data := make([]byte, len(bs)+HeaderSize)
		copy(data[:HeaderSize], v.data[:HeaderSize])
		v.data = data
	}
	copy(v.data[HeaderSize:], bs)
}
func (v Value) Valid() bool {
	if v.Type() == NIL {
		return false
	}
	if len(v.data) <= HeaderSize {
		return false
	}
	switch v.t {
	case BoolType:
		if len(v.data) != HeaderSize+1 {
			return false
		}
	case IntType, FloatType:
		if len(v.data) != HeaderSize+8 {
			return false
		}
	}
	return true
}

func (v Value) ExpireAt() uint64 {
	return v.e
}

func (v Value) Expired(now uint64) bool {
	if v.e == 0 {
		return false
	}
	return now > v.e
}

func (v Value) Type() uint8 {
	switch v.t {
	case BoolType, IntType, FloatType, StrType:
		return v.t
	default:
		return NIL
	}
}

func (v Value) String() string {
	if len(v.data) <= HeaderSize {
		return ""
	}
	switch v.t {
	case NIL:
		return ""
	case BoolType:
		if v.b {
			return "true"
		}
		return "false"
	case IntType:
		return strconv.FormatInt(v.i, 10)
	case FloatType:
		return strconv.FormatFloat(v.f, 'f', '-', 64)
	}
	return string(v.data[HeaderSize:])
}
func (v Value) Bool() (bool, bool) {
	return v.b, v.t == BoolType
}

func (v Value) Int() (int64, bool) {
	return v.i, v.t == IntType
}
func (v Value) Float() (float64, bool) {
	return v.f, v.t == FloatType
}
func (v Value) Bytes() []byte {
	if len(v.data) <= HeaderSize {
		return nil
	}
	return v.data[HeaderSize:]
}

func (v Value) StringVal() []byte {
	return StringToBytes(v.String())
}

func (v Value) SavedData() []byte {
	return v.data
}
func (v Value) Raw() []byte {
	return v.data
}

const (
	NIL       uint8 = 0
	BoolType  uint8 = 0b00000001
	IntType   uint8 = 0b00000010
	FloatType uint8 = 0b00000011
	StrType   uint8 = 0b00000100
)
const (
	MinNumByte   = '0'
	MaxNumByte   = '9'
	DotByte      = '.'
	NegativeByte = '-'
)

func getValType(val []byte) uint8 {
	var size = len(val)
	if size == 0 {
		return NIL
	}

	dotCount := 0

	var isInt, isFloat = true, true
	var start = 0
	if val[0] == NegativeByte {
		start = 1
	}

	for i, v := range val[start:] {
		if isInt || isFloat {
			if v < MinNumByte || v > MaxNumByte {
				isInt = false
				if v != DotByte {
					isFloat = false
				} else {
					dotCount++
					if dotCount > 1 {
						return StrType
					}
					if i == 0 || i == size-1 {
						return StrType
					}
				}
			}
			continue
		}
		return StrType
	}
	if isInt {
		return IntType
	}
	if isFloat {
		return FloatType
	}
	return StrType
}
