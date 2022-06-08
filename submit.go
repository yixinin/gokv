package gokv

import (
	"fmt"
	"strconv"

	"github.com/yixinin/gokv/codec"
)

type CommitOP int

const (
	CommitOPSet   CommitOP = 1
	CommitOPDel   CommitOP = 2
	CommitOPExDel CommitOP = 3
)

func (t CommitOP) String() string {
	switch t {
	case CommitOPSet:
		return "set"
	case CommitOPDel:
		return "del"
	case CommitOPExDel:
		return "exdel"
	}
	return strconv.Itoa(int(t))
}

// Command a raft op command
type Submit struct {
	OP    CommitOP `json:"op"`
	Key   []byte   `json:"k"`
	Value []byte   `json:"v,omitempty"`
}

func (c *Submit) String() string {
	switch c.OP {
	case CommitOPSet:
		return fmt.Sprintf("Set %s %s", c.Key, c.Value)
	case CommitOPDel:
		return fmt.Sprintf("Delete %s", c.Key)
	case CommitOPExDel:
		return fmt.Sprintf("ExDel %s", c.Key)
	default:
		return "<Invalid>"
	}
}
func (c *Submit) UniqueKey() string {
	return fmt.Sprintf("%s:%s:%s", c.OP, c.Key, c.Value)
}
func (c *Submit) Valid() bool {
	if c == nil {
		return false
	}
	if c.Key == nil || (c.OP != CommitOPDel && c.OP != CommitOPSet) {
		return false
	}
	return true
}

func NewSetSubmit(key, val []byte, ex ...uint64) *Submit {
	ct := &Submit{
		OP:  CommitOPSet,
		Key: key,
	}
	data := codec.Encode(val, ex...)
	ct.Value = data.Raw()
	return ct
}
func NewSetRawSubmit(key, data []byte) *Submit {
	return &Submit{
		OP:    CommitOPSet,
		Key:   key,
		Value: data,
	}
}

func NewDelSubmit(key []byte) *Submit {
	return &Submit{
		OP:  CommitOPDel,
		Key: key,
	}
}

func NewExDelSubmit(key []byte) *Submit {
	return &Submit{
		OP:  CommitOPExDel,
		Key: key,
	}
}
