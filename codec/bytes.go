package codec

import (
	"bytes"
	"encoding/binary"
)

func Int642Bytes(i int64) []byte {
	bytesBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(bytesBuffer, binary.BigEndian, i)
	return bytesBuffer.Bytes()
}

func Bytes2Int64(b []byte) int64 {
	var i int64
	binary.Read(bytes.NewBuffer(b), binary.BigEndian, &i)
	return i
}

func Float2Bytes(f float64) []byte {
	bytesBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(bytesBuffer, binary.BigEndian, f)
	return bytesBuffer.Bytes()
}

func Bytes2Float(b []byte) float64 {
	var f float64
	binary.Read(bytes.NewBuffer(b), binary.BigEndian, &f)
	return f
}
