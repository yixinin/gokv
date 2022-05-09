package logger

import (
	"time"

	"github.com/sirupsen/logrus"
)

const ModuleKey = "module"

type basicHook struct {
	appName string
}

func (*basicHook) Levels() []logrus.Level {
	return []logrus.Level{
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
	if b.appName != "" {
		e.Data["app"] = b.appName
	}
	if e.Context != nil {
		module, _ := e.Context.Value(ModuleKey).(string)
		if module != "" {
			e.Data[ModuleKey] = module
		}
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

func Hooks() []logrus.Hook {
	return []logrus.Hook{
		&basicHook{},
	}
}
