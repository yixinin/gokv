package logger

import (
	"context"
	"io"

	"github.com/sirupsen/logrus"
)

var (
	err = logrus.New()
)

func init() {
	err.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetFormatter(&logrus.JSONFormatter{})
}

var EnbaleErrorLogger bool

func SetOutput(stdW, errW io.Writer) {
	logrus.SetOutput(stdW)
	if errW != nil {
		err.SetOutput(errW)
	}
}

func SetLevel(lvl logrus.Level) {
	logrus.SetLevel(lvl)
	err.SetLevel(logrus.ErrorLevel)
}

func AddHook(hook logrus.Hook) {
	logrus.AddHook(hook)
	err.AddHook(hook)
}

func StandardLogger() *logrus.Logger {
	return logrus.StandardLogger()
}

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
	for _, v := range args {
		if err, ok := v.(error); ok {
			log = log.WithError(err)
			break
		}
	}
	if EnbaleErrorLogger {
		err.WithFields(log.Data).Error(args...)
	}
	log.Error(args...)
}

func Errorln(ctx context.Context, args ...interface{}) {
	var log = logrus.WithContext(ctx)
	for _, v := range args {
		if err, ok := v.(error); ok {
			log = log.WithError(err)
			break
		}
	}
	if EnbaleErrorLogger {
		err.WithFields(log.Data).Errorln(args...)
	}
	log.Errorln(args...)
}

func Errorf(ctx context.Context, format string, args ...interface{}) {
	var log = logrus.WithContext(ctx)
	for _, v := range args {
		if err, ok := v.(error); ok {
			log = log.WithError(err)
			break
		}
	}
	if EnbaleErrorLogger {
		logrus.WithFields(log.Data).Errorf(format, args...)
	}
	log.Errorf(format, args...)
}
