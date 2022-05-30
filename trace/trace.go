package trace

import (
	"context"
	"encoding/hex"

	"github.com/bwmarrin/snowflake"
)

type traceKey struct {
}

func (traceKey) String() string {
	return "trace"
}

type spanKey struct {
}

func (spanKey) String() string {
	return "span"
}

var (
	TraceKey = traceKey{}
	SpanKey  = spanKey{}
)
var node *snowflake.Node

func init() {
	node, _ = snowflake.NewNode(1)
}

func GenTrace() string {
	var sb = node.Generate().IntBytes()
	var tb = node.Generate().IntBytes()
	var buf = make([]byte, 16)
	copy(buf[:8], sb[:])
	copy(buf[8:], tb[:])
	traceID := hex.EncodeToString(buf)
	return traceID
}
func GenSpan() string {
	var sb = node.Generate().IntBytes()
	return hex.EncodeToString(sb[:])
}

func WithTrace(ctx context.Context) context.Context {
	trace := GenTrace()
	if ctx.Value(SpanKey) == nil {
		ctx = context.WithValue(ctx, SpanKey, trace[:16])
	}
	if ctx.Value(TraceKey) == nil {
		ctx = context.WithValue(ctx, TraceKey, trace)
	}
	return ctx
}
func WithSpan(ctx context.Context) context.Context {
	if ctx.Value(SpanKey) != nil {
		return ctx
	}
	return context.WithValue(ctx, SpanKey, GenSpan())
}

// CopyContext return a new context with src traceid and spanid
func CopyContext(src context.Context) context.Context {
	ctx := context.Background()
	trace := src.Value(TraceKey)
	span := src.Value(SpanKey)
	if trace != nil {
		ctx = context.WithValue(ctx, TraceKey, trace)
	}
	if span != nil {
		ctx = context.WithValue(ctx, SpanKey, span)
	}
	return ctx
}

func TraceID(ctx context.Context) string {
	s, _ := ctx.Value(TraceKey).(string)
	return s
}

func SpanID(ctx context.Context) string {
	s, _ := ctx.Value(SpanKey).(string)
	return s
}
