package stats

import (
	"sync"
	"time"

	raft "go.etcd.io/etcd/raft/v3"
)

type ServerStats struct {
	serverStats
	sync.Mutex
}

func (*ServerStats) SendAppendReq(int) {

}

type LeaderStats struct {
	serverStats
	sync.Mutex
}

type FollowerStats struct {
}

func (*LeaderStats) Follower(string) {

}

type serverStats struct {
	Name string `json:"name"`
	// ID is the raft ID of the node.
	// TODO(jonboulle): use ID instead of name?
	ID        string         `json:"id"`
	State     raft.StateType `json:"state"`
	StartTime time.Time      `json:"startTime"`

	LeaderInfo struct {
		Name      string    `json:"leader"`
		Uptime    string    `json:"uptime"`
		StartTime time.Time `json:"startTime"`
	} `json:"leaderInfo"`

	RecvAppendRequestCnt uint64  `json:"recvAppendRequestCnt,"`
	RecvingPkgRate       float64 `json:"recvPkgRate,omitempty"`
	RecvingBandwidthRate float64 `json:"recvBandwidthRate,omitempty"`

	SendAppendRequestCnt uint64  `json:"sendAppendRequestCnt"`
	SendingPkgRate       float64 `json:"sendPkgRate,omitempty"`
	SendingBandwidthRate float64 `json:"sendBandwidthRate,omitempty"`

	sendRateQueue *statsQueue
	recvRateQueue *statsQueue
}

func NewServerStats(id string) *ServerStats {
	return nil
}

func NewLeaderStats(id string) *LeaderStats {
	return nil
}
