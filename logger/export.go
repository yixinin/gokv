package logger

import (
	"context"

	"github.com/sirupsen/logrus"
)

func WithField(k string, v interface{}) *Entry {
	return &Entry{
		logrus.WithField(k, v),
	}
}

func WithFields(fs logrus.Fields) *Entry {
	return &Entry{
		logrus.WithFields(fs),
	}
}

func Debug(ctx context.Context, args ...interface{}) {
	logrus.WithContext(ctx).Debug(args...)
}

func Debugln(ctx context.Context, args ...interface{}) {
	logrus.WithContext(ctx).Debugln(args...)
}

func Debugf(ctx context.Context, format string, args ...interface{}) {
	logrus.WithContext(ctx).Debugf(format, args...)
}

func Info(ctx context.Context, args ...interface{}) {
	logrus.WithContext(ctx).Info(args...)
}

func Infoln(ctx context.Context, args ...interface{}) {
	logrus.WithContext(ctx).Infoln(args...)
}

func Infof(ctx context.Context, format string, args ...interface{}) {
	logrus.WithContext(ctx).Infof(format, args...)
}

func Warning(ctx context.Context, args ...interface{}) {
	logrus.WithContext(ctx).Warning(args...)
}

func Warningln(ctx context.Context, args ...interface{}) {
	logrus.WithContext(ctx).Warningln(args...)
}

func Warningf(ctx context.Context, format string, args ...interface{}) {
	logrus.WithContext(ctx).Warningf(format, args...)
}

func Error(ctx context.Context, args ...interface{}) {
	var log = logrus.WithContext(ctx)
	log.Error(args...)
}

func Errorln(ctx context.Context, args ...interface{}) {
	var log = logrus.WithContext(ctx)
	log.Errorln(args...)
}

func Errorf(ctx context.Context, format string, args ...interface{}) {
	var log = logrus.WithContext(ctx)

	log.Error(args...)
}
