package msgbuzz

import (
	"fmt"
	"log"
)

type Logger interface {
	Debug(message string)
	Debugf(format string, args ...interface{})
	Info(message string)
	Infof(message string, args ...interface{})
	Warning(message string)
	Warningf(message string, args ...interface{})
	Error(message string)
	Errorf(message string, args ...interface{})
}

type NoOpLogger struct {
}

func NewNoOpLogger() *NoOpLogger {
	return &NoOpLogger{}
}

func (n *NoOpLogger) Debug(message string) {
	// Intentionally blank
}

func (n *NoOpLogger) Debugf(format string, args ...interface{}) {
	// Intentionally blank
}

func (n *NoOpLogger) Info(message string) {
	// Intentionally blank
}

func (n *NoOpLogger) Infof(message string, args ...interface{}) {
	// Intentionally blank
}

func (n *NoOpLogger) Warning(message string) {
	// Intentionally blank
}

func (n *NoOpLogger) Warningf(message string, args ...interface{}) {
	// Intentionally blank
}

func (n *NoOpLogger) Error(message string) {
	// Intentionally blank
}

func (n *NoOpLogger) Errorf(message string, args ...interface{}) {
	// Intentionally blank
}

type LogLevel int

const Debug = LogLevel(0)
const Info = LogLevel(1)
const Warning = LogLevel(2)
const Error = LogLevel(3)

type DefaultLogger struct {
	LogLevel LogLevel
}

func NewDefaultLogger(level LogLevel) *DefaultLogger {
	return &DefaultLogger{
		LogLevel: level,
	}
}

func (d *DefaultLogger) print(message string) {
	log.Println(message)
}
func (d *DefaultLogger) printf(format string, args ...interface{}) {
	log.Println(fmt.Sprintf(format, args...))
}

func (d *DefaultLogger) Debug(message string) {
	if d == nil {
		return
	}
	if d.LogLevel <= Debug {
		d.print(message)
	}
}

func (d *DefaultLogger) Debugf(format string, args ...interface{}) {
	if d == nil {
		return
	}
	if d.LogLevel <= Debug {
		d.printf(format, args...)
	}
}

func (d *DefaultLogger) Info(message string) {
	if d == nil {
		return
	}
	if d.LogLevel <= Info {
		d.print(message)
	}
}

func (d *DefaultLogger) Infof(format string, args ...interface{}) {
	if d == nil {
		return
	}
	if d.LogLevel <= Info {
		d.printf(format, args...)
	}
}

func (d *DefaultLogger) Warning(message string) {
	if d == nil {
		return
	}
	if d.LogLevel <= Warning {
		d.print(message)
	}
}

func (d *DefaultLogger) Warningf(format string, args ...interface{}) {
	if d == nil {
		return
	}
	if d.LogLevel <= Warning {
		d.printf(format, args...)
	}
}

func (d *DefaultLogger) Error(message string) {
	if d == nil {
		return
	}
	if d.LogLevel <= Error {
		d.print(message)
	}
}

func (d *DefaultLogger) Errorf(format string, args ...interface{}) {
	if d == nil {
		return
	}
	if d.LogLevel <= Error {
		d.printf(format, args...)
	}
}
