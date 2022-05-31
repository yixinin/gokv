package protocol

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/yixinin/gokv/codec"
	"github.com/yixinin/gokv/kverror"
	"github.com/yixinin/gokv/logger"
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
	*ErrResp
	Now     uint64
	Command string
	Key     []byte
	args    [][]byte
}

type ErrResp struct {
	Err error
}

func (c *ErrResp) Write(w *Writer) error {
	return w.writeError(c.Err)
}

type OkResp struct {
	OK bool
}

func (r *OkResp) Write(w *Writer) error {
	if r.OK {
		return w.bytes(StatusReply, OK)
	}
	return w.bytes(ErrorReply, []byte("Fail"))
}

func Command(ctx context.Context, args []interface{}) *BaseCmd {
	var base = &BaseCmd{
		Now:     uint64(time.Now().Unix()),
		ErrResp: &ErrResp{},
		args:    make([][]byte, 1, len(args)),
	}
	if logger.EnableDebug() {
		var debugStr = make([]string, 0, len(args))
		for _, v := range args {
			debugStr = append(debugStr, codec.BytesToString(v.([]byte)))
		}
		logger.Debugf(ctx, "cmd %v", strings.Join(debugStr, " "))
	}

	size := len(args)
	if size == 0 {
		base.Err = kverror.ErrCommandArgs
		return base
	}
	var ok bool
	base.args[0], ok = args[0].([]byte)
	if !ok {
		base.Err = kverror.ErrCommandArgs
		return base
	}
	base.Command = codec.BytesToString(base.args[0])
	for i := 1; i < size; i++ {
		switch arg := args[i].(type) {
		case []byte:
			base.args = append(base.args, arg)
		case int64:
			base.args = append(base.args, codec.StringToBytes(strconv.FormatInt(arg, 10)))
		case string:
			base.args = append(base.args, codec.StringToBytes(arg))
		default:
			base.Err = kverror.ErrCommandArgs
			return base
		}
	}

	if size >= 2 {
		base.Key = base.args[1]
	}

	return base
}

type SetCmd struct {
	*BaseCmd
	*OkResp

	Val []byte

	EX     uint64
	KEEPEX bool
	DEL    bool

	NX bool
}

func NewSetCmd(base *BaseCmd) *SetCmd {
	var size = len(base.args)
	var cmd = &SetCmd{
		BaseCmd: base,
		OkResp:  &OkResp{},
	}
	if size < 3 {
		cmd.Err = kverror.ErrCommandArgs
		return cmd
	}

	cmd.Val = base.args[2]
	cmd.KEEPEX = true
	for i := 3; i < size; i++ {
		arg := base.args[i]
		switch codec.BytesToString(arg) {
		case EX:
			if size >= i+2 {
				ex, _ := codec.StringBytes2Int64(base.args[i+1])
				if ex > 0 {
					cmd.EX = uint64(ex) + base.Now
				}
				cmd.DEL = ex < 0
				cmd.KEEPEX = ex == 0
				i++
			}
		case NX:
			cmd.NX = true
		}
	}
	return cmd
}

func (c *SetCmd) Write(w *Writer) error {
	if c.Err != nil {
		return c.ErrResp.Write(w)
	}
	return c.OkResp.Write(w)
}

type GetCmd struct {
	*BaseCmd

	Val []byte
}

func NewGetCmd(base *BaseCmd) *GetCmd {
	cmd := &GetCmd{
		BaseCmd: base,
	}
	return cmd
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

func NewDelCmd(base *BaseCmd) *DelCmd {
	cmd := &DelCmd{
		BaseCmd: base,
		OkResp:  &OkResp{},
	}
	return cmd
}

type ExpireCmd struct {
	*BaseCmd
	*OkResp
	EX  uint64
	Del bool
}

func NewExpirecmd(base *BaseCmd) *ExpireCmd {
	var cmd = &ExpireCmd{
		BaseCmd: base,
		OkResp:  &OkResp{},
	}
	if len(base.args) < 3 {
		cmd.Err = kverror.ErrCommandArgs
		return cmd
	}

	ex, _ := codec.StringBytes2Int64(base.args[2])
	cmd.Del = ex <= 0
	if ex > 0 {
		cmd.EX = uint64(ex) + base.Now
	}

	return cmd
}

func (c *ExpireCmd) Write(w *Writer) error {
	if c.Err != nil {
		return c.ErrResp.Write(w)
	}
	if c.OK {
		return w.int(1)
	}
	return w.int(0)
}

type TTLCmd struct {
	*BaseCmd
	TTL int64
}

func NewTTLCmd(base *BaseCmd) *TTLCmd {
	cmd := &TTLCmd{
		BaseCmd: base,
	}
	return cmd
}

func (c *TTLCmd) Write(w *Writer) error {
	if c.Err != nil {
		return c.ErrResp.Write(w)
	}
	return w.int(c.TTL)
}

type IncrByCmd struct {
	*BaseCmd
	Val int64
}

func NewIncrCmd(base *BaseCmd) *IncrByCmd {
	var cmd = &IncrByCmd{
		Val:     1,
		BaseCmd: base,
	}
	return cmd
}

func NewDecrCmd(base *BaseCmd) *IncrByCmd {
	var cmd = &IncrByCmd{
		Val:     -1,
		BaseCmd: base,
	}
	return cmd
}

func NewIncrByCmd(base *BaseCmd) *IncrByCmd {
	var cmd = &IncrByCmd{
		BaseCmd: base,
	}
	if len(base.args) < 3 {
		cmd.Err = kverror.ErrCommandArgs
		return cmd
	}

	val, _ := codec.StringBytes2Int64(base.args[2])
	if val != 0 {
		cmd.Val = val
		return cmd
	}
	return cmd
}

func NewDecrByCmd(base *BaseCmd) *IncrByCmd {
	var cmd = &IncrByCmd{
		BaseCmd: base,
	}
	if len(base.args) < 3 {
		cmd.Err = kverror.ErrCommandArgs
		return cmd
	}

	val, _ := codec.StringBytes2Int64(base.args[2])
	if val != 0 {
		cmd.Val = -val
		return cmd
	}
	return cmd
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
	var vals = map[string]CommandInfo{
		"get": {
			ReadOnly: true,
			Name:     "get",
			Arity:    2,
			Flags: []string{
				"readonly",
				"random",
				"fast",
			},
			FirstKeyPos: 1,
			LastKeyPos:  1,
			StepCount:   1,
			ACLFlags: []string{
				"@read",
				"@string",
				"@fast",
			},
		},
		"set": {
			ReadOnly: false,
			Name:     "set",
			Arity:    -3,
			Flags: []string{
				"write",
				"denyoom",
			},
			FirstKeyPos: 1,
			LastKeyPos:  1,
			StepCount:   1,
			ACLFlags: []string{
				"@write",
				"@string",
				"@slow",
			},
		},
		"del": {
			ReadOnly: false,
			Name:     "del",
			Arity:    -2,
			Flags: []string{
				"write",
				"denyoom",
			},
			FirstKeyPos: 1,
			LastKeyPos:  -1,
			StepCount:   1,
			ACLFlags: []string{
				"@keyspace",
				"@write",
				"@slow",
			},
		},
		"expire": {
			ReadOnly: false,
			Name:     "expire",
			Arity:    3,
			Flags: []string{
				"write",
				"slow",
			},
			FirstKeyPos: 1,
			LastKeyPos:  1,
			StepCount:   1,
			ACLFlags: []string{
				"@keyspace",
				"@write",
				"@slow",
			},
		},
		"ttl": {
			ReadOnly: true,
			Name:     "ttl",
			Arity:    2,
			Flags: []string{
				"readonly",
				"random",
				"fast",
			},
			FirstKeyPos: 1,
			LastKeyPos:  1,
			StepCount:   1,
			ACLFlags: []string{
				"@keyspace",
				"@read",
				"@fast",
			},
		},
		"incr": {
			ReadOnly: false,
			Name:     "incr",
			Arity:    2,
			Flags: []string{
				"write",
				"denyoom",
				"slow",
			},
			FirstKeyPos: 1,
			LastKeyPos:  1,
			StepCount:   1,
			ACLFlags: []string{
				"@write",
				"@string",
				"@slow",
			},
		},
		"incrby": {
			ReadOnly: false,
			Name:     "incrby",
			Arity:    3,
			Flags: []string{
				"write",
				"denyoom",
				"slow",
			},
			FirstKeyPos: 1,
			LastKeyPos:  1,
			StepCount:   1,
			ACLFlags: []string{
				"@write",
				"@string",
				"@slow",
			},
		},
		"decr": {
			ReadOnly: false,
			Name:     "decr",
			Arity:    2,
			Flags: []string{
				"write",
				"denyoom",
				"slow",
			},
			FirstKeyPos: 1,
			LastKeyPos:  1,
			StepCount:   1,
			ACLFlags: []string{
				"@write",
				"@string",
				"@slow",
			},
		},
		"decrby": {
			ReadOnly: false,
			Name:     "decrby",
			Arity:    2,
			Flags: []string{
				"write",
				"denyoom",
				"slow",
			},
			FirstKeyPos: 1,
			LastKeyPos:  1,
			StepCount:   1,
			ACLFlags: []string{
				"@write",
				"@string",
				"@slow",
			},
		},
	}

	return &CommandsInfoCmd{
		Val: vals,
	}
}

func (c *CommandsInfoCmd) Write(w *Writer) error {
	if err := w.WriteByte(ArrayReply); err != nil {
		return err
	}
	if err := w.writeLen(len(c.Val)); err != nil {
		return err
	}

	for _, info := range c.Val {
		if err := w.WriteByte(ArrayReply); err != nil {
			return err
		}
		if err := w.writeLen(7); err != nil {
			return err
		}
		if info.ReadOnly {
			info.Flags = append(info.Flags, "readonly")
		}
		// name
		if err := w.bytes(StringReply, codec.StringToBytes(info.Name)); err != nil {
			return err
		}
		// arity
		if err := w.int(int64(info.Arity)); err != nil {
			return err
		}
		//flag
		if err := w.writeArray(StatusReply, info.Flags...); err != nil {
			return err
		}

		if err := w.int(int64(info.FirstKeyPos)); err != nil {
			return err
		}
		if err := w.int(int64(info.LastKeyPos)); err != nil {
			return err
		}
		if err := w.int(int64(info.StepCount)); err != nil {
			return err
		}
		//acl flag
		if err := w.writeArray(StatusReply, info.ACLFlags...); err != nil {
			return err
		}
	}
	return nil
}

type SentinelCmd struct {
	SubCmd     string
	MasterAddr [2]string
	SlaveAddrs map[int][]string
}

func NewSentinelCmd(args []interface{}) *SentinelCmd {
	if len(args) < 2 {
		return &SentinelCmd{
			SlaveAddrs: make(map[int][]string, 2),
		}
	}
	return &SentinelCmd{
		SubCmd:     codec.BytesToString(args[1].([]byte)),
		SlaveAddrs: make(map[int][]string, 2),
	}
}

func (c *SentinelCmd) Write(w *Writer) error {
	switch c.SubCmd {
	case "sentinels":
		w.WriteByte(ArrayReply)
		w.writeLen(len(c.SlaveAddrs) + 1)
		var master = make([]string, 4)
		master[0] = "ip"
		master[1] = c.MasterAddr[0]
		master[2] = "port"
		master[3] = c.MasterAddr[1]
		w.writeArray(StringReply, master...)
		for _, v := range c.SlaveAddrs {
			w.writeArray(StringReply, v...)
		}
	case "get-master-addr-by-name":
		return w.writeArray(StringReply, c.MasterAddr[:]...)
	case "slaves":
		w.WriteByte(ArrayReply)
		w.writeLen(len(c.SlaveAddrs))
		for _, v := range c.SlaveAddrs {
			w.writeArray(StringReply, v...)
		}
	}
	return nil
}

type PingCommand struct {
}

func (c *PingCommand) Write(w *Writer) error {
	return w.bytes(StatusReply, PONG)
}
