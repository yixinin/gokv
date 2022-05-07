package kvstore

import (
	"fmt"
	"testing"
)

func TestData(t *testing.T) {
	var i = "false"
	bs := Data2Bytes(i, 0)
	fmt.Println(bs)
	_, f := Bytes2Data(bs)
	fmt.Println(f)
}
