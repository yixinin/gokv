package logger

import (
	"context"

	"github.com/sirupsen/logrus"
)

type LoggerInterface interface {
	Debug(ctx context.Context, args ...interface{})
	Debugf(ctx context.Context, format string, args ...interface{})
	Debugln(ctx context.Context, args ...interface{})
	Info(ctx context.Context, args ...interface{})
	Infof(ctx context.Context, format string, args ...interface{})
	Infoln(ctx context.Context, args ...interface{})
	Warning(ctx context.Context, args ...interface{})
	Warningf(ctx context.Context, format string, args ...interface{})
	Warningln(ctx context.Context, args ...interface{})
	Error(ctx context.Context, args ...interface{})
	Errorf(ctx context.Context, format string, args ...interface{})
	Errorln(ctx context.Context, args ...interface{})
	WithField(string, interface{}) *Entry
	WithFields(logrus.Fields) *Entry
}
