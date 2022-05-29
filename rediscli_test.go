package gokv

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func TestRedisCli(t *testing.T) {
	c := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName: "xx",
		SentinelAddrs: []string{
			"localhost:6479",
			"localhost:6579",
			"localhost:6679",
		},
	})

	for i := 0; i < 1000; i++ {
		ok, err := c.Set(context.Background(), "x1", "xx", 10*time.Second).Result()
		fmt.Println(time.Now(), ok, err)
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
