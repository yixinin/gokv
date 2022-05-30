package logger

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yixinin/gokv/kverror"
	"github.com/yixinin/gokv/trace"
)

const ModuleKey = "module"

type basicHook struct {
	appName string
}

func (*basicHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.FatalLevel,
		logrus.PanicLevel,
		logrus.ErrorLevel,
		logrus.FatalLevel,
		logrus.PanicLevel,
		logrus.DebugLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
	}
}
func (b *basicHook) Fire(e *logrus.Entry) error {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	return nil
}

type traceHook struct {
}

func (*traceHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.FatalLevel,
		logrus.PanicLevel,
		logrus.ErrorLevel,
		logrus.FatalLevel,
		logrus.PanicLevel,
		logrus.DebugLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
	}
}

func (*traceHook) Fire(e *logrus.Entry) error {
	if e.Context == nil {
		return nil
	}
	ctx := e.Context
	traceVal, _ := ctx.Value(trace.TraceKey).(string)
	if traceVal != "" {
		e.Data[trace.TraceKey.String()] = traceVal
	}
	spanVal, _ := ctx.Value(trace.SpanKey).(string)
	if spanVal != "" {
		e.Data[trace.SpanKey.String()] = spanVal
	}
	return nil
}

type memCacheHook struct {
}

func (*memCacheHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}
func (b *memCacheHook) Fire(e *logrus.Entry) error {
	if _, ok := e.Data["mem_hit"]; ok {
		//TODO statistics
	}
	return nil
}

type errorHook struct {
}

func (*errorHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.ErrorLevel,
		logrus.FatalLevel,
		logrus.PanicLevel,
	}
}
func (b *errorHook) Fire(e *logrus.Entry) error {
	if err, ok := e.Data[logrus.ErrorKey]; ok {
		if terr, ok := err.(*kverror.KvError); ok {
			e.Data["stacks"] = terr.GetStacks()
		}
	}
	return nil
}

func Hooks() []logrus.Hook {
	return []logrus.Hook{
		&basicHook{},
		// &memCacheHook{},
		&traceHook{},
		&errorHook{},
	}
}
