package storage

import (
	"fmt"
	"testing"
)

func TestData(t *testing.T) {
	var i = "false"
	bs := String2Bytes(i, 0)
	fmt.Println(bs)
	_, f := Bytes2String(bs)
	fmt.Println(f)
}
