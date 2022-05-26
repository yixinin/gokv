package gokv

import (
	"fmt"
	"strconv"
)

type CmdType int

const (
	// CmdQuorumGet quorum get a key
	CmdGet CmdType = 1
	// CmdPut put key value
	CmdSet CmdType = 2
	// CmdDelete delete a key
	CmdDelete CmdType = 3
)

func (t CmdType) String() string {
	switch t {
	case CmdGet:
		return "get"
	case CmdSet:
		return "set"
	case CmdDelete:
		return "del"
	}
	return strconv.Itoa(int(t))
}

// Command a raft op command
type Command struct {
	OP    CmdType `json:"op"`
	Key   []byte  `json:"k"`
	Value []byte  `json:"v"`
}

func (c *Command) String() string {
	switch c.OP {
	case CmdGet:
		return fmt.Sprintf("Get %v", string(c.Key))
	case CmdSet:
		return fmt.Sprintf("Set %s %s", string(c.Key), string(c.Value))
	case CmdDelete:
		return fmt.Sprintf("Delete %s", string(c.Key))
	default:
		return "<Invalid>"
	}
}
