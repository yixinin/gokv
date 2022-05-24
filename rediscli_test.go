package gokv

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-redis/redis/v8"
)

func TestRedisCli(t *testing.T) {
	c := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	x, err := c.Command(context.Background()).Result()

	// buf, _ := json.Marshal(res)
	fmt.Println(x, err)

}
