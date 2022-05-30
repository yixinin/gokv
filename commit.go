package gokv

import (
	"fmt"
	"strconv"

	"github.com/yixinin/gokv/codec"
)

type CommitOP int

const (
	CommitOPSet CommitOP = 1
	CommitOPDel CommitOP = 2
)

func (t CommitOP) String() string {
	switch t {
	case CommitOPSet:
		return "set"
	case CommitOPDel:
		return "del"
	}
	return strconv.Itoa(int(t))
}

// Command a raft op command
type Commit struct {
	OP    CommitOP `json:"op"`
	Key   []byte   `json:"k"`
	Value []byte   `json:"v"`
}

func (c *Commit) String() string {
	switch c.OP {
	case CommitOPSet:
		return fmt.Sprintf("Set %s %s", string(c.Key), string(c.Value))
	case CommitOPDel:
		return fmt.Sprintf("Delete %s", string(c.Key))
	default:
		return "<Invalid>"
	}
}

func NewSetCommit(key, val []byte, ex ...uint64) *Commit {
	ct := &Commit{
		OP:  CommitOPSet,
		Key: key,
	}
	data := codec.Encode(val, ex...)
	ct.Value = data.Raw()
	return ct
}
func NewSetRawCommit(key, data []byte) *Commit {
	return &Commit{
		OP:    CommitOPSet,
		Key:   key,
		Value: data,
	}
}

func NewDelCommit(key []byte) *Commit {
	return &Commit{
		OP:  CommitOPDel,
		Key: key,
	}
}
