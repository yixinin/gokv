package codec

var defaultEncoder = byesEncoder{}
var defaultDecoder = bytesDecoder{}

func Encode(s string, ex ...uint64) Value {
	return defaultEncoder.Encode(s, ex...)
}

func Decode(data []byte) Value {
	return defaultDecoder.Decode(data)
}
