package protocol

import (
	"time"

	"github.com/yixinin/gokv/codec"
)

var (
	EX = "ex"
	PX = "px"
	NX = "nx"
)

var OK = []byte("OK")

type Responser interface {
	Write(c *Writer) (int, error)
}

type BaseCmd struct {
	Now uint64
	Key []byte
}

type ErrResp struct {
	Err error
}

func (c *ErrResp) Write(w *Writer) error {
	if c.Err != nil {
		return w.writeError(c.Err)
	}
	return nil
}

type OkResp struct {
	*ErrResp
	OK bool
}

func NewOkResp() *OkResp {
	return &OkResp{
		ErrResp: &ErrResp{},
	}
}

func (r *OkResp) Write(w *Writer) error {
	if r.OK {
		return w.bytes(StatusReply, OK)
	}
	if r.Err != nil {
		return r.ErrResp.Write(w)
	}
	return w.bytes(ErrorReply, []byte("Fail"))
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
	*OkResp

	Val []byte
	EX  uint64
	NX  bool
}

func NewSetCmd(args []interface{}) (*SetCmd, bool) {
	var size = len(args)
	var cmd = &SetCmd{
		OkResp: NewOkResp(),
	}
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
	*ErrResp

	Val []byte
}

func NewGetCmd(args []interface{}) (*GetCmd, bool) {
	c, ok := NewBaseCmd(args)
	cmd := &GetCmd{
		BaseCmd: c,
		ErrResp: &ErrResp{},
	}
	return cmd, ok
}

func (c *GetCmd) Write(w *Writer) error {
	if c.Err != nil {
		return c.ErrResp.Write(w)
	}
	return w.bytes(StringReply, c.Val)
}

type DelCmd struct {
	*BaseCmd
	*OkResp
}

func NewDelCmd(args []interface{}) (*DelCmd, bool) {
	c, ok := NewBaseCmd(args)
	cmd := &DelCmd{
		BaseCmd: c,
		OkResp:  NewOkResp(),
	}
	return cmd, ok
}

type ExpireCmd struct {
	*BaseCmd
	*OkResp
	EX uint64
}

func NewExpirecmd(args []interface{}) (*ExpireCmd, bool) {
	var cmd = &ExpireCmd{
		OkResp: NewOkResp(),
	}
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

type TTLCmd struct {
	*BaseCmd
	*ErrResp
	TTL int64
}

func NewTTLCmd(args []interface{}) (*TTLCmd, bool) {
	c, ok := NewBaseCmd(args)
	cmd := &TTLCmd{
		BaseCmd: c,
		ErrResp: &ErrResp{},
	}
	return cmd, ok
}

func (c *TTLCmd) Write(w *Writer) error {
	if c.Err != nil {
		return c.ErrResp.Write(w)
	}
	return w.int(c.TTL)
}

type IncrByCmd struct {
	*BaseCmd
	*ErrResp
	Val int64
}

func NewIncrCmd(args []interface{}) (*IncrByCmd, bool) {
	var cmd = &IncrByCmd{
		Val:     1,
		ErrResp: &ErrResp{},
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

func NewIncrByCmd(args []interface{}) (*IncrByCmd, bool) {
	var cmd = &IncrByCmd{
		ErrResp: &ErrResp{},
	}
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
	if c.Err != nil {
		return c.ErrResp.Write(w)
	}
	return w.int(c.Val)
}

type CommandInfo struct {
	Name        string
	Arity       int8
	Flags       []string
	ACLFlags    []string
	FirstKeyPos int8
	LastKeyPos  int8
	StepCount   int8
	ReadOnly    bool
}

type CommandsInfoCmd struct {
	Val map[string]CommandInfo
}

func NewCommandsInfoCmd() *CommandsInfoCmd {
	return &CommandsInfoCmd{
		Val: map[string]CommandInfo{
			"get":    {},
			"set":    {},
			"del":    {},
			"expire": {},
			"ttl":    {},
			"incr":   {},
			"incrby": {},
			"decr":   {},
			"decrby": {},
			"keys":   {},
			"scan":   {},
		},
	}
}

func (c *CommandsInfoCmd) Write(w *Writer) error {
	err := w.writeArray("no")
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
			w.writeArray(v...)
		}
	case "get-master-addr-by-name":
		return w.writeArray(c.MasterAddr[:]...)
	}
	return nil
}
