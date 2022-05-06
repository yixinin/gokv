package storage

import (
	"fmt"
	"testing"
)

func TestData(t *testing.T) {
	var i = "false"
	bs := String2Bytes(i)
	fmt.Println(bs)
	f := Bytes2String(bs)
	fmt.Println(f)
}
