package gokv_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

var client *redis.Client

func TestGet(t *testing.T) {
	var key = "k1"
	err := client.Get(context.Background(), key).Err()
	if err != nil && err != redis.Nil {
		t.Error(err)
	}
}

func TestSet(t *testing.T) {
	var k1 = "k1"
	val := rand.Int()
	err := client.Set(context.Background(), k1, val, time.Second).Err()
	if err != nil {
		t.Error(err)
	}
	err = client.SetNX(context.Background(), "l2", val, time.Second).Err()
	if err != nil && err != redis.Nil {
		t.Error(err)
	}
}

func TestIncr(t *testing.T) {
	err := client.Incr(context.Background(), "k4").Err()
	if err != nil {
		t.Error(err)
	}
}

func TestGokvBenchMark(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	client = redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName: "xx",
		SentinelAddrs: []string{
			"localhost:6479",
			"localhost:6579",
			"localhost:6679",
		},
	})

	var p = 1000
	var wg sync.WaitGroup
	var bench = func(t *testing.T) {
		TestSet(t)
		TestIncr(t)
		TestGet(t)
		wg.Done()
	}

	wg.Add(p)
	var start = time.Now()
	for i := 0; i < p; i++ {
		go bench(t)
	}

	wg.Wait()
	ms := time.Since(start).Milliseconds()
	fmt.Println("total ms:", ms)
}

func TestRedisBenchMark(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	client = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	var p = 10000
	var wg sync.WaitGroup
	var bench = func(t *testing.T) {
		TestSet(t)
		TestIncr(t)
		TestGet(t)
		wg.Done()
	}

	wg.Add(p)
	var start = time.Now()
	for i := 0; i < p; i++ {
		go bench(t)
	}

	wg.Wait()
	ms := time.Since(start).Milliseconds()
	fmt.Println("total ms:", ms)
}
