package logrus

import (
	"github.com/sirupsen/logrus"
)

type LogrusLogger struct {
	ext     string
	log     *logrus.Entry
	context map[string]interface{}
}

func NewLogger(extFieldsKey string) *LogrusLogger {
	// activate json logging by default
	logrus.SetFormatter(&JSONFormatterEx{})
	return &LogrusLogger{
		ext: extFieldsKey,
		log: logrus.WithFields(map[string]interface{}{}),
	}
}

func (l *LogrusLogger) UseTextFormatter() {
	logrus.SetFormatter(&logrus.TextFormatter{})
}

func (l *LogrusLogger) SetLevel(level int) {
	logrus.SetLevel(logrus.Level(level))
}

func (l *LogrusLogger) SetContext(context map[string]interface{}) {
	l.context = context
	l.log = logrus.WithFields(l.context)
}

func (l *LogrusLogger) GetContext() map[string]interface{} {
	return l.context
}

func (l *LogrusLogger) Println(args ...interface{}) {
	l.log.Println(args...)
}

func (l *LogrusLogger) Printf(format string, args ...interface{}) {
	l.log.Printf(format, args...)
}

func (l *LogrusLogger) Trace(msg string, fields map[string]interface{}) {
	l.log.WithFields(map[string]interface{}{l.ext: fields}).Trace(msg)
}

func (l *LogrusLogger) Debug(msg string, fields map[string]interface{}) {
	l.log.WithFields(map[string]interface{}{l.ext: fields}).Debug(msg)
}

func (l *LogrusLogger) Info(msg string, fields map[string]interface{}) {
	l.log.WithFields(map[string]interface{}{l.ext: fields}).Info(msg)
}

func (l *LogrusLogger) Warning(msg string, fields map[string]interface{}) {
	l.log.WithFields(map[string]interface{}{l.ext: fields}).Warning(msg)
}

func (l *LogrusLogger) Error(msg string, fields map[string]interface{}) {
	l.log.WithFields(map[string]interface{}{l.ext: fields}).Error(msg)
}

func (l *LogrusLogger) Fatal(msg string, fields map[string]interface{}) {
	l.log.WithFields(map[string]interface{}{l.ext: fields}).Fatal(msg)
}
