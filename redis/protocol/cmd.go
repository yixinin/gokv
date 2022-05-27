package protocol

import (
	"time"

	"github.com/tiglabs/raft/util/log"
	"github.com/yixinin/gokv/codec"
)

var (
	EX = "ex"
	PX = "px"
	NX = "nx"
)

type Commander interface {
	Write(c *Writer) (int, error)
}

type BaseCmd struct {
	Now uint64
	Key []byte
}

func NewBaseCmd(args []interface{}) (*BaseCmd, bool) {
	var cmd = &BaseCmd{
		Now: uint64(time.Now().Unix()),
	}
	if len(args) < 2 {
		return cmd, false
	}
	cmd.Key, _ = args[1].([]byte)
	return cmd, true
}

type SetCmd struct {
	*BaseCmd
	Val []byte
	EX  uint64
	NX  bool

	Message string
}

func NewSetCmd(args []interface{}) (*SetCmd, bool) {
	var size = len(args)
	var cmd = &SetCmd{}
	if size < 3 {
		return cmd, false
	}
	c, ok := NewBaseCmd(args)
	cmd.BaseCmd = c
	cmd.Val, _ = args[2].([]byte)
	for i := 3; i < size; i++ {
		arg, ok := args[i].([]byte)
		if !ok {
			continue
		}
		switch codec.BytesToString(arg) {
		case EX:
			if size >= i+2 {
				ex, _ := codec.StringBytes2Int64(args[i+1].([]byte))
				if ex > 0 {
					cmd.EX = uint64(ex) + c.Now
				}
				i++
			}
		case NX:
			cmd.NX = true
		}
	}
	return cmd, ok
}

type GetCmd struct {
	*BaseCmd

	Val     []byte
	Message string
	Nil     bool
}

func NewGetCmd(args []interface{}) (*GetCmd, bool) {
	c, ok := NewBaseCmd(args)
	cmd := &GetCmd{
		BaseCmd: c,
	}
	return cmd, ok
}

func (c *GetCmd) Write(w *Writer) error {
	if c.Nil {
		return w.nil(StringReply)
	}
	if c.Message != "" {
		return w.string(c.Message)
	}
	return w.bytes(StringReply, c.Val)
}

type DelCmd struct {
	*BaseCmd
	Message string
}

func NewDelCmd(args []interface{}) (*DelCmd, bool) {
	c, ok := NewBaseCmd(args)
	cmd := &DelCmd{
		BaseCmd: c,
	}
	return cmd, ok
}

type ExpireCmd struct {
	*BaseCmd
	EX      uint64
	Message string
}

func NewExpirecmd(args []interface{}) (*ExpireCmd, bool) {
	var cmd = &ExpireCmd{}
	if len(args) < 3 {
		return cmd, false
	}
	c, ok := NewBaseCmd(args)
	if !ok {
		return cmd, false
	}
	cmd.BaseCmd = c
	switch arg := args[2].(type) {
	case int64:
		if arg > 0 {
			cmd.EX = uint64(arg) + c.Now
			return cmd, true
		}
	case []byte:
		ex, _ := codec.StringBytes2Int64(arg)
		if ex > 0 {
			cmd.EX = uint64(ex) + c.Now
			return cmd, true
		}
	}
	return cmd, false
}
func (c *ExpireCmd) Write(w *Writer) error {
	return w.string(c.Message)
}

type TTLCmd struct {
	*BaseCmd
	Message string
	TTL     int64
}

func NewTTLCmd(args []interface{}) (*TTLCmd, bool) {
	c, ok := NewBaseCmd(args)
	cmd := &TTLCmd{
		BaseCmd: c,
	}
	return cmd, ok
}

func (c *TTLCmd) Write(w *Writer) error {
	if c.Message != "" {
		log.Debug("ttl got error msg", c.Message)
		return w.string(c.Message)
	}
	return w.int(c.TTL)
}

func NewIncrCmd(args []interface{}) (*IncrByCmd, bool) {
	var cmd = &IncrByCmd{
		Val: 1,
	}
	if len(args) < 2 {
		return cmd, false
	}
	c, ok := NewBaseCmd(args)
	if !ok {
		return cmd, false
	}
	cmd.BaseCmd = c
	return cmd, ok
}

type IncrByCmd struct {
	*BaseCmd
	Val     int64
	Message string
}

func NewIncrByCmd(args []interface{}) (*IncrByCmd, bool) {
	var cmd = &IncrByCmd{}
	if len(args) < 3 {
		return cmd, false
	}
	c, ok := NewBaseCmd(args)
	if !ok {
		return cmd, false
	}
	cmd.BaseCmd = c
	switch arg := args[2].(type) {
	case int64:
		if arg != 0 {
			cmd.Val = arg
			return cmd, true
		}
	case []byte:
		val, _ := codec.StringBytes2Int64(arg)
		if val != 0 {
			cmd.Val = val
			return cmd, true
		}
	}
	return cmd, false
}

func (c *IncrByCmd) Write(w *Writer) error {
	if c.Message != "" {
		log.Debug("incr got error msg", c.Message)
		return w.string(c.Message)
	}
	return w.int(c.Val)
}

type CommandsInfoCmd struct {
	Val map[string]interface{}
}

func NewCommandsInfoCmd() *CommandsInfoCmd {
	return &CommandsInfoCmd{}
}

func (c *CommandsInfoCmd) Write(w *Writer) error {
	err := w.WriteMessage("no")
	return err
}

type SentinelCmd struct {
	SubCmd     string
	MasterAddr [2]string
	SlaveAddrs [][]string
}

func NewSentinelCmd(args []interface{}) *SentinelCmd {
	if len(args) < 2 {
		return &SentinelCmd{}
	}
	return &SentinelCmd{
		SubCmd: codec.BytesToString(args[1].([]byte)),
	}
}

func (c *SentinelCmd) Write(w *Writer) error {
	switch c.SubCmd {
	case "sentinels":
		w.WriteByte(ArrayReply)
		w.writeLen(len(c.SlaveAddrs))
		for _, v := range c.SlaveAddrs {
			w.WriteArray(v...)
		}
	case "get-master-addr-by-name":
		return w.WriteArray(c.MasterAddr[:]...)
	}
	return nil
}
