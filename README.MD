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
// chmod +x build.sh
./build.sh

cd node1
./cmd -node=1

cd ../node2
./cmd -node=2

cd ../node3
./cmd -node=3

```

## client

``` go
    client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName: "xx",
		SentinelAddrs: []string{
			"localhost:6479",
			"localhost:6579",
			"localhost:6679",
		},
	})

```