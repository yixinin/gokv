package gokv

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/yixinin/gokv/codec"
)

func TestRedisCli(t *testing.T) {
	c := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName: "xx",
		SentinelAddrs: []string{
			"localhost:9001",
			"localhost:9002",
			"localhost:9003",
		},
	})

	for i := 0; i < 1000; i++ {
		ok, err := c.Set(context.Background(), "x1", "xx", 10*time.Second).Result()
		fmt.Println(time.Now(), ok, err)
		okk, err := c.SetNX(context.Background(), "xx1", "xxx", 2*time.Second).Result()
		fmt.Println(okk, err)
		// x, err := c.Command(context.Background()).Result()
		x, err := c.Get(context.Background(), "x1").Result()
		// buf, _ := json.Marshal(res)
		fmt.Println(time.Now(), x, err)
		i, err := c.Incr(context.Background(), "x2").Result()
		fmt.Println(i, err)
		ttl, err := c.TTL(context.Background(), "x3").Result()
		fmt.Println(ttl, err)
		ttl, err = c.TTL(context.Background(), "x2").Result()
		fmt.Println(ttl, err)
		ttl, err = c.TTL(context.Background(), "x1").Result()
		fmt.Println(ttl, err)
		time.Sleep(1 * time.Second)
	}

}

func TestBytesAdd(t *testing.T) {
	var x1 int64 = math.MaxInt - 255
	var x2 int64 = 254

	b1 := codec.Int642Bytes(x1)
	b2 := codec.Int642Bytes(x2)
	fmt.Println(b1)
	fmt.Println(b2)
	sum := bytesAdd(b1, b2)
	fmt.Println(codec.Bytes2Int64(sum))
	fmt.Println("except", codec.Int642Bytes(x1+x2))
	fmt.Println("result", sum)

}

func TestNx(t *testing.T) {
	c := redis.NewFailoverClusterClient(&redis.FailoverOptions{
		MasterName: "xx",
		SentinelAddrs: []string{
			"localhost:9001",
			"localhost:9002",
			"localhost:9003",
		},
		RouteRandomly: true,
		SlaveOnly:     false,
	})
	ok, err := c.SetNX(context.Background(), "key1", "", 2*time.Second).Result()
	fmt.Println(ok, err)
}
