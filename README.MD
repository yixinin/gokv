# GOKV

## Implmented Command
- ping
- get
- set [ex, nx]
- del
- expire
- ttl
- incr, incrby
- decr, decrby
- sentinel

## How to use
``` sh
cd cmd

go build

./cmd -node=1

./cmd -node=2

./cmd -node=3

```

## client

``` go
    client := redis.NewFailoverClusterClient(&redis.FailoverOptions{
		MasterName: "xx",
		SentinelAddrs: []string{
			"localhost:6679",
			"localhost:6579",
			"localhost:6479",
		},
		RouteRandomly: true,
		SlaveOnly:     false,
	})

```