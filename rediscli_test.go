package gokv

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func TestRedisCli(t *testing.T) {
	c := redis.NewClient(&redis.Options{
		Addr: "localhost:6378",
	})
	s, err := c.Ping(context.Background()).Result()
	t.Error(s, err)
	s, err = c.Set(context.Background(), "k", "v", time.Second).Result()
	t.Error(s, err)
	// x, err := c.Set(context.Background(), "k", "v", 1).Result()
	// t.Log(x, err)

}
