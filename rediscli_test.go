package gokv

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-redis/redis/v8"
)

func TestRedisCli(t *testing.T) {
	c := redis.NewClient(&redis.Options{
		Addr: "localhost:6579",
	})
	// ok, err := c.Set(context.Background(), "x", "xx", 10*time.Second).Result()
	// fmt.Println(ok, err)
	// x, err := c.Command(context.Background()).Result()
	x, err := c.Get(context.Background(), "x").Result()
	// buf, _ := json.Marshal(res)
	fmt.Println(x, err)

}
