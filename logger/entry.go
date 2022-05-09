package logger

import (
	"context"

	"github.com/sirupsen/logrus"
)

type Entry struct {
	e *logrus.Entry
}

func (e *Entry) Debug(ctx context.Context, args ...interface{}) {
	e.e.WithContext(ctx).Debug(args...)
}
func (e *Entry) Debugf(ctx context.Context, format string, args ...interface{}) {
	e.e.WithContext(ctx).Debugf(format, args...)
}
func (e *Entry) Debugln(ctx context.Context, args ...interface{}) {
	e.e.WithContext(ctx).Debugln(args...)
}
func (e *Entry) Info(ctx context.Context, args ...interface{}) {
	e.e.WithContext(ctx).Info(args...)
}
func (e *Entry) Infof(ctx context.Context, format string, args ...interface{}) {
	e.e.WithContext(ctx).Infof(format, args...)
}
func (e *Entry) Infoln(ctx context.Context, args ...interface{}) {
	e.e.WithContext(ctx).Infoln(args...)
}
func (e *Entry) Warning(ctx context.Context, args ...interface{}) {
	e.e.WithContext(ctx).Warning(args...)
}
func (e *Entry) Warningf(ctx context.Context, format string, args ...interface{}) {
	e.e.WithContext(ctx).Warningf(format, args...)
}
func (e *Entry) Warningln(ctx context.Context, args ...interface{}) {
	e.e.WithContext(ctx).Warningln(args...)
}
func (e *Entry) Error(ctx context.Context, args ...interface{}) {
	var log = e.e.WithContext(ctx)
	log.Error(args...)
}
func (e *Entry) Errorf(ctx context.Context, format string, args ...interface{}) {
	var log = e.e.WithContext(ctx)
	log.Errorf(format, args...)
}
func (e *Entry) Errorln(ctx context.Context, args ...interface{}) {
	var log = e.e.WithContext(ctx)
	log.Errorln(args...)
}
func (e *Entry) WithField(k string, v interface{}) *Entry {
	return &Entry{
		e.e.WithField(k, v),
	}
}
func (e *Entry) WithFields(fs logrus.Fields) *Entry {
	return &Entry{
		e.e.WithFields(fs),
	}
}
