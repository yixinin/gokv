package protocol

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
	if len(args) >= 4 {
		ex, ok := args[3].(int64)
		if ok {
			cmd.Expire = uint64(ex)
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
