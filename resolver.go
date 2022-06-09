package gokv

import (
	"fmt"

	"github.com/yixinin/raft"
)

// ClusterResolver implement raft Resolver
type ClusterResolver struct {
	cfg *Config
}

func newClusterResolver(cfg *Config) *ClusterResolver {
	return &ClusterResolver{
		cfg: cfg,
	}
}

// NodeAddress get node address
func (r *ClusterResolver) NodeAddress(nodeID uint64, stype raft.SocketType) (addr string, err error) {
	node := r.cfg.FindClusterNode(nodeID)
	if node == nil {
		return "", fmt.Errorf("could not find node(%v) in cluster config:\n: %v", nodeID, r.cfg.String())
	}
	switch stype {
	case raft.HeartBeat:
		return fmt.Sprintf("%s:%d", node.Host, node.HeartbeatPort), nil
	case raft.Replicate:
		return fmt.Sprintf("%s:%d", node.Host, node.ReplicatePort), nil
	}
	return "", fmt.Errorf("unknown socket type: %v", stype)
}
