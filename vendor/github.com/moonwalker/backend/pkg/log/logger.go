package log

import (
	"github.com/moonwalker/backend/pkg/log/logrus"
	"github.com/moonwalker/logger"
)

const (
	EXT_FIELDS = "properties"
)

const (
	PanicLevel = iota
	FatalLevel
	ErrorLevel
	WarnLevel
	InfoLevel
	DebugLevel
	TraceLevel
)

// helper
type Fields map[string]interface{}

type Log struct {
	logger *logrus.LogrusLogger
}

func New() logger.Logger {
	return Log{
		logger: logrus.NewLogger(EXT_FIELDS),
	}
}

// default logger, `logrus`
var DefaultLogger = logrus.NewLogger(EXT_FIELDS)

func (l Log) UseTextFormatter() {
	l.logger.UseTextFormatter()
}

func (l Log) SetLevel(level int) {
	l.logger.SetLevel(level)
}

func (l Log) SetContext(context map[string]interface{}) {
	l.logger.SetContext(context)
}

func (l Log) GetContext() map[string]interface{} {
	return l.logger.GetContext()
}

func (l Log) Println(args ...interface{}) {
	l.logger.Println(args...)
}

func (l Log) Printf(format string, args ...interface{}) {
	l.logger.Printf(format, args...)
}

func (l Log) Trace(msg string, fields map[string]interface{}) {
	l.logger.Trace(msg, fields)
}

func (l Log) Debug(msg string, fields map[string]interface{}) {
	l.logger.Debug(msg, fields)
}

func (l Log) Info(msg string, fields map[string]interface{}) {
	l.logger.Info(msg, fields)
}

func (l Log) Warning(msg string, fields map[string]interface{}) {
	l.logger.Warning(msg, fields)
}

func (l Log) Error(msg string, fields map[string]interface{}) {
	l.logger.Error(msg, fields)
}

func (l Log) Fatal(msg string, fields map[string]interface{}) {
	l.logger.Fatal(msg, fields)
}

func UseTextFormatter() {
	DefaultLogger.UseTextFormatter()
}

func SetLevel(level int) {
	DefaultLogger.SetLevel(level)
}

func SetContext(context map[string]interface{}) {
	DefaultLogger.SetContext(context)
}

func GetContext() map[string]interface{} {
	return DefaultLogger.GetContext()
}

func Println(args ...interface{}) {
	DefaultLogger.Println(args...)
}

func Printf(format string, args ...interface{}) {
	DefaultLogger.Printf(format, args...)
}

func Trace(msg string, fields map[string]interface{}) {
	DefaultLogger.Trace(msg, fields)
}

func Debug(msg string, fields map[string]interface{}) {
	DefaultLogger.Debug(msg, fields)
}

func Info(msg string, fields map[string]interface{}) {
	DefaultLogger.Info(msg, fields)
}

func Warning(msg string, fields map[string]interface{}) {
	DefaultLogger.Warning(msg, fields)
}

func Error(msg string, fields map[string]interface{}) {
	DefaultLogger.Error(msg, fields)
}

func Fatal(msg string, fields map[string]interface{}) {
	DefaultLogger.Fatal(msg, fields)
}
