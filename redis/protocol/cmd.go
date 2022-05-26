package protocol

import (
	"strconv"
	"time"
)

type Command interface {
}

type SetCmd struct {
	Key    string
	Val    string
	Expire uint64
}

func NewSetCmd(args []interface{}) (SetCmd, bool) {
	var cmd SetCmd
	if len(args) < 3 {
		return cmd, false
	}
	cmd.Key, _ = args[1].(string)
	cmd.Val, _ = args[2].(string)
	for i := 3; i < len(args); i++ {
		if a, ok := args[i].(string); ok {
			switch a {
			case "ex":
				if len(args) >= i+2 {
					ex, _ := strconv.ParseInt(args[i+1].(string), 10, 64)
					if ex > 0 {
						cmd.Expire = uint64(time.Now().Unix() + ex)
					}
					i++
				}
			}
		}
	}
	return cmd, true
}

type KeyCmd struct {
	Key string
}

func NewKeyCmd(args []interface{}) (KeyCmd, bool) {
	var cmd KeyCmd
	if len(args) < 2 {
		return cmd, false
	}
	cmd.Key, _ = args[1].(string)
	return cmd, true
}

type Expirecmd struct {
	Key    string
	Expire uint64
}

func NewExpirecmd(args []interface{}) (Expirecmd, bool) {
	var cmd Expirecmd
	if len(args) < 3 {
		return cmd, false
	}
	cmd.Key, _ = args[1].(string)
	ex, ok := args[2].(int64)
	if !ok {
		return cmd, false
	}
	cmd.Expire = uint64(ex)
	return cmd, true
}
