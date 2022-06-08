package codec

import (
	"bytes"
	"encoding/binary"
	"strconv"
)

const (
	TypeSize   = 1
	ExpireSize = 8
	NumberSize = 8
	HeaderSize = 9
)

func Int642Bytes(i int64) []byte {
	bytesBuffer := bytes.NewBuffer(make([]byte, 0, ExpireSize))
	binary.Write(bytesBuffer, binary.BigEndian, i)
	return bytesBuffer.Bytes()
}
func Uint642Bytes(i uint64) []byte {
	bytesBuffer := bytes.NewBuffer(make([]byte, 0, ExpireSize))
	binary.Write(bytesBuffer, binary.BigEndian, i)
	return bytesBuffer.Bytes()
}

func Bytes2Int64(b []byte) int64 {
	var i int64
	binary.Read(bytes.NewBuffer(b), binary.BigEndian, &i)
	return i
}

func StringBytes2Int64(b []byte) (int64, bool) {
	s := BytesToString(b)
	i, err := strconv.ParseInt(s, 10, 64)
	return i, err == nil
}

func StringBytes2Uint64(b []byte) (uint64, bool) {
	s := string(b)
	i, err := strconv.ParseUint(s, 10, 64)
	return i, err == nil
}
func Float2Bytes(f float64) []byte {
	bytesBuffer := bytes.NewBuffer(make([]byte, 0, ExpireSize))
	binary.Write(bytesBuffer, binary.BigEndian, f)
	return bytesBuffer.Bytes()
}

func Bytes2Float(b []byte) float64 {
	var f float64
	binary.Read(bytes.NewBuffer(b), binary.BigEndian, &f)
	return f
}

func BytesEq(s1, s2 []byte) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}
