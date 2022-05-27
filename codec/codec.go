package codec

var defaultEncoder = byesEncoder{}
var defaultDecoder = bytesDecoder{}

func Encode(s []byte, ex ...uint64) Value {
	return defaultEncoder.Encode(s, ex...)
}

func EncodeInt(i int64, ex ...uint64) Value {
	return defaultEncoder.EncodeInt(i, ex...)
}

func Decode(data []byte) Value {
	return defaultDecoder.Decode(data)
}
