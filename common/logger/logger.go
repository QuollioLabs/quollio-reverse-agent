package logger

import (
	"fmt"
	"log"
	"os"
	"runtime"
)

const (
	ERROR = iota + 1
	WARNING
	INFO
	DEBUG
)

func SetLogLevel() int {
	logLevel := os.Getenv("LOG_LEVEL")
	switch logLevel {
	case "INFO", "info":
		return INFO
	case "DEBUG", "debug":
		return DEBUG
	case "ERROR", "error":
		return ERROR
	case "WARNING", "warning":
		return WARNING
	default:
		return INFO
	}
}

type BuiltinLogger struct {
	logger *log.Logger
	level  int
}

func NewBuiltinLogger() *BuiltinLogger {
	return &BuiltinLogger{
		logger: log.Default(),
		level:  SetLogLevel(),
	}
}

func (l *BuiltinLogger) Debug(format string, args ...interface{}) {
	if l.level >= DEBUG {
		prefix := "[DEBG] "
		l.logger.SetOutput(os.Stdout)
		l.logger.SetPrefix(prefix)
		l.logger.SetFlags(log.Ldate | log.Ltime)

		_, file, line, ok := runtime.Caller(1)
		if ok {
			caller := fmt.Sprintf("@%s:%d: ", file, line)
			l.logger.Printf(caller+format, args...)
		} else {
			l.logger.Printf(format, args...)
		}
	}
}

func (l *BuiltinLogger) Info(format string, args ...interface{}) {
	if l.level >= INFO {
		prefix := "[INFO] "
		l.logger.SetOutput(os.Stdout)
		l.logger.SetPrefix(prefix)
		l.logger.SetFlags(log.Ldate | log.Ltime)
		l.logger.Printf(format, args...)
	}
}

func (l *BuiltinLogger) Warning(format string, args ...interface{}) {
	if l.level >= WARNING {
		prefix := "[WARN] "
		l.logger.SetOutput(os.Stdout)
		l.logger.SetPrefix(prefix)
		l.logger.SetFlags(log.Ldate | log.Ltime)
		l.logger.Printf(format, args...)
	}
}

func (l *BuiltinLogger) Error(format string, args ...interface{}) {
	if l.level >= ERROR {
		prefix := "[EROR] "
		l.logger.SetOutput(os.Stdout)
		l.logger.SetPrefix(prefix)
		l.logger.SetFlags(log.Ldate | log.Ltime)

		_, file, line, ok := runtime.Caller(1)
		if ok {
			caller := fmt.Sprintf("@%s:%d: ", file, line)
			l.logger.Printf(caller+format, args...)
		} else {
			l.logger.Printf(format, args...)
		}
	}
}

func (l *BuiltinLogger) Fatal(format string, args ...interface{}) {
	if l.level >= ERROR {
		prefix := "[EROR] "
		l.logger.SetOutput(os.Stdout)
		l.logger.SetPrefix(prefix)
		l.logger.SetFlags(log.Ldate | log.Ltime)

		_, file, line, ok := runtime.Caller(1)
		if ok {
			caller := fmt.Sprintf("@%s:%d: ", file, line)
			l.logger.Fatalf(caller+format, args...)
		} else {
			l.logger.Fatalf(format, args...)
		}
	}
}
