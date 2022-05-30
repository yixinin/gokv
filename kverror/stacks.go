package kverror

import (
	"fmt"
	"runtime"
	"strings"
)

func (e *KvError) GetStacks() string {
	return e.Stack
}

const maxDepthStack = 25

func getStacks() string {
	var sb = strings.Builder{}
	for i := 2; i < maxDepthStack; i += 1 {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		if strings.Contains(file, "/pkg/mod") {
			break
		}
		if strings.Contains(file, "/go/src/") {
			break
		}
		name := runtime.FuncForPC(pc).Name()
		n := strings.LastIndex(name, "/")
		if n > 0 && n <= len(name) {
			name = name[n+1:]
		}
		sb.WriteString(fmt.Sprintf("%s:%d\n\tfunc:%s\n", file, line, name))
	}
	return sb.String()
}
