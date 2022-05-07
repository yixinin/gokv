package stats

import "sync"

type statsQueue struct {
	items        [queueCapacity]*RequestStats
	size         int
	front        int
	back         int
	totalReqSize int
	rwl          sync.RWMutex
}
