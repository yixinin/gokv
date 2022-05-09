package gokv_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/yixinin/gokv"
	"github.com/yixinin/gokv/kvstore/leveldb"
)

func TestRedis(t *testing.T) {
	db, err := leveldb.NewStorage("tmp/lvl")
	if err != nil {
		t.Error(err)
		return
	}
	var s = gokv.NewServer(db)
	var ctx = context.Background()
	r, err := s.Incr(ctx, "asd", "1")
	fmt.Println(r, err)
	r, err = s.Incr(ctx, "asd", "2")
	fmt.Println(r, err)

	err = s.HSet(ctx, "hk1", "hf3", "hv3")
	fmt.Println(err)
	hv, err := s.HGetAll(ctx, "hk1")
	fmt.Println(hv, err)
}
