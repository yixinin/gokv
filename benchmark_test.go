package gokv_test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

var client *redis.Client

func TestGet(t *testing.T) {
	var key = "k1:" + strconv.Itoa(rand.Intn(100))
	err := client.Get(context.Background(), key).Err()
	if err != nil && err != redis.Nil {
		t.Error(err)
	}

}

func TestSet(t *testing.T) {
	var key = "k1:" + strconv.Itoa(rand.Intn(100))
	val := rand.Int()
	err := client.Set(context.Background(), key, val, time.Second).Err()
	if err != nil {
		t.Error(err)
	}
	key = "l2:" + strconv.Itoa(rand.Intn(10))
	err = client.SetNX(context.Background(), key, val, time.Second).Err()
	if err != nil && err != redis.Nil {
		t.Error(err)
	}
}

func TestIncr(t *testing.T) {
	key := "k4:" + strconv.Itoa(rand.Intn(10))
	err := client.Incr(context.Background(), key).Err()
	if err != nil {
		t.Error(err)
	}
}

func TestConcurrency(t *testing.T) {
	client = redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName: "xx",
		SentinelAddrs: []string{
			"localhost:6479",
			"localhost:6579",
			"localhost:6679",
		},
	})
	for i := 0; i < 100; i++ {
		var ctx = context.Background()
		go client.Incr(ctx, "x1")
	}
	time.Sleep(time.Second)
	v, err := client.Get(context.Background(), "x1").Result()
	fmt.Println(v, err)
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
		UseDisconnectedSlaves: true,
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

func TestRandom(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	client := redis.NewFailoverClusterClient(&redis.FailoverOptions{
		MasterName: "xx",
		SentinelAddrs: []string{
			"localhost:6679",
			"localhost:6579",
			"localhost:6479",
		},
		RouteRandomly: true,
	})
	for i := 0; i < 100; i++ {
		if x, err := client.TTL(context.TODO(), "k1").Result(); err != nil || x != -2 {
			fmt.Println("not nil", x, err)
			return
		}
		ok, err := client.Set(context.TODO(), "k1", "va1", time.Second).Result()
		if err != nil {
			t.Error(ok, err)
		}
		v, err := client.Get(context.TODO(), "k1").Result()
		switch err {
		case redis.Nil:
			fmt.Println("not found")
		case nil:
			fmt.Println(v)
		default:
			t.Error(err)
		}

		time.Sleep(time.Second + time.Millisecond)

	}
}

func TestSlaveOnly(t *testing.T) {
	// TestMaster(t)
	client = redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName: "xx",
		SentinelAddrs: []string{
			"localhost:6679",
			"localhost:6579",
			"localhost:6479",
		},
		SlaveOnly: true,
	})
	for i := 0; i < 100; i++ {
		TestGet(t)
	}

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
