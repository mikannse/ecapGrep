package logger

import (
	"log"
)

type Logger struct {
	DebugEnabled bool
}

var loggerInstance = &Logger{DebugEnabled: true}

func (l *Logger) Debug(format string, args ...interface{}) {
	if l.DebugEnabled {
		log.Printf("[DEBUG] "+format, args...)
	}
}

func (l *Logger) Error(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

func Debug(format string, args ...interface{}) {
	loggerInstance.Debug(format, args...)
}

func Error(format string, args ...interface{}) {
	loggerInstance.Error(format, args...)
}
