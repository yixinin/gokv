package kvstore

import (
	"bytes"
	"encoding/binary"
	"strconv"
)

const (
	NIL       uint8 = 0
	BoolType  uint8 = 0b0001
	IntType   uint8 = 0b0010
	FloatType uint8 = 0b0011
	StrType   uint8 = 0b0100
)
const (
	min         = '0'
	max         = '9'
	dot         = '.'
	negativeSig = '-'
)

func String2Bytes(val string, expireAt uint64) []byte {
	var data []byte
	var typ uint8
	switch val {
	case "true", "True", "TRUE":
		typ = BoolType
		data = []byte{1}
	case "false", "False", "FALSE":
		typ = BoolType
		data = []byte{0}
	default:
		typ = getValType(val)
		switch typ {
		default:
			data = []byte(val)
		case FloatType:
			f, _ := strconv.ParseFloat(val, 64)
			bytesBuffer := bytes.NewBuffer(make([]byte, 0, 8))
			binary.Write(bytesBuffer, binary.BigEndian, f)
			data = bytesBuffer.Bytes()
		case IntType:
			i, _ := strconv.ParseInt(val, 10, 64)
			bytesBuffer := bytes.NewBuffer(make([]byte, 0, 8))
			binary.Write(bytesBuffer, binary.BigEndian, i)
			data = bytesBuffer.Bytes()
		}
	}
	setData := make([]byte, len(data)+1+8)
	setData[0] = typ

	expireAtBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(expireAtBuf, expireAt)

	copy(setData[1:9], expireAtBuf)
	copy(setData[9:], data)
	return setData
}
func Bytes2String(data []byte) (expireAt uint64, s string) {
	var size = len(data)
	if size < 2+8 {
		return 0, ""
	}
	switch data[0] {
	case BoolType:
		if data[9] == 1 {
			s = "true"
		}
		s = "false"
	case IntType:
		var i int64
		binary.Read(bytes.NewBuffer(data[9:]), binary.BigEndian, &i)
		s = strconv.FormatInt(i, 10)
	case FloatType:
		var f float64
		binary.Read(bytes.NewBuffer(data[9:]), binary.BigEndian, &f)
		s = strconv.FormatFloat(f, 'f', -1, 64)
	default:
		s = string(data[9:])
	}
	expireAt = binary.BigEndian.Uint64(data[1:9])
	return
}
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
