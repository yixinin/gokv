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

	raw []byte

	e uint64
}

func (v Value) CopyFrom(nv Value) {
	v.b = nv.b
	v.e = nv.e
	v.t = nv.t
	v.f = nv.f
	v.i = nv.i
	v.raw = nv.raw
}

func (v Value) SetExpireAt(ex uint64) {
	v.e = ex
	eb := make([]byte, 8)
	binary.BigEndian.PutUint64(eb, ex)
	copy(v.raw[1:9], eb)

}

func (v Value) Set(val string) {
	nv := Encode(val, v.ExpireAt())
	v.CopyFrom(nv)
}

func (v Value) SetInt(val int64) {
	v.t = IntType
	v.SetBytes(Int642Bytes(val))
}

func (v Value) SetFloat(f float64) {
	v.t = FloatType
	v.SetBytes(Float2Bytes(f))
}

func (v Value) SetBool(b bool) {
	bs := make([]byte, 1)
	if b {
		bs[0] = 1
	}
	v.t = BoolType
	v.SetBytes(bs)
}
func (v Value) SetBytes(bs []byte) {
	if len(v.raw)-9 != len(bs) {
		data := make([]byte, len(bs)+9)
		copy(data[:9], v.raw[:9])
		v.raw = data
	}
	copy(v.raw[9:], bs)
}
func (v Value) Valid() bool {
	if v.Type() == NIL {
		return false
	}
	if len(v.raw) <= 9 {
		return false
	}
	switch v.t {
	case BoolType:
		if len(v.raw) != 1+8+1 {
			return false
		}
	case IntType, FloatType:
		if len(v.raw) != 1+8+8 {
			return false
		}
	}
	return true
}

func (v Value) ExpireAt() uint64 {
	return v.e
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
	if len(v.raw) <= 9 {
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
	return string(v.raw[9:])
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
	if len(v.raw) <= 9 {
		return nil
	}
	return v.raw[9:]
}

func (v Value) SavedData() []byte {
	return v.raw
}

const (
	NIL       uint8 = 0
	BoolType  uint8 = 0b00000001
	IntType   uint8 = 0b00000010
	FloatType uint8 = 0b00000011
	StrType   uint8 = 0b00000100
)
const (
	min         = '0'
	max         = '9'
	dot         = '.'
	negativeSig = '-'
)

func getValType(val string) uint8 {
	var size = len(val)
	if size == 0 {
		return NIL
	}

	dotCount := 0

	var isInt, isFloat = true, true
	var start = 0
	if val[0] == negativeSig {
		start = 1
	}

	for i, v := range val[start:] {
		if isInt || isFloat {
			if v < min || v > max {
				isInt = false
				if v != dot {
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
