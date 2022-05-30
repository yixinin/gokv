package logger

import (
	"github.com/sirupsen/logrus"
)

type RaftLogger struct {
	std *logrus.Logger
	err *logrus.Logger
}

func NewRaftLogger() *RaftLogger {
	return &RaftLogger{
		err: err,
		std: logrus.StandardLogger(),
	}
}

func (l *RaftLogger) IsEnableDebug() bool {
	return l.std.Level >= logrus.DebugLevel
}
func (l *RaftLogger) IsEnableInfo() bool {
	return l.std.Level >= logrus.InfoLevel
}
func (l *RaftLogger) IsEnableWarn() bool {
	return l.std.Level >= logrus.WarnLevel
}

func (l *RaftLogger) Debug(format string, v ...interface{}) {
	l.std.Debugf(format, v...)
}
func (l *RaftLogger) Info(format string, v ...interface{}) {
	l.std.Infof(format, v...)
}
func (l *RaftLogger) Warn(format string, v ...interface{}) {
	l.std.Warnf(format, v...)
}
func (l *RaftLogger) Error(format string, v ...interface{}) {
	l.err.Errorf(format, v...)
}
