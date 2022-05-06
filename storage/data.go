package storage

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

func String2Bytes(val string) []byte {
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
	setData := make([]byte, len(data)+1)
	setData[0] = typ
	copy(setData[1:], data)
	return setData
}
func Bytes2String(data []byte) string {
	var size = len(data)
	if size < 2 {
		return ""
	}
	switch data[0] {
	case BoolType:
		if data[1] == 1 {
			return "true"
		}
		return "false"
	case IntType:
		var i int64
		binary.Read(bytes.NewBuffer(data[1:]), binary.BigEndian, &i)
		return strconv.FormatInt(i, 10)
	case FloatType:
		var f float64
		binary.Read(bytes.NewBuffer(data[1:]), binary.BigEndian, &f)
		return strconv.FormatFloat(f, 'f', -1, 64)
	default:
		return string(data[1:])
	}
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
