package stats

import "sync"

const queueCapacity = 10

type RequestStats struct {
}
type statsQueue struct {
	items        [queueCapacity]*RequestStats
	size         int
	front        int
	back         int
	totalReqSize int
	rwl          sync.RWMutex
}
