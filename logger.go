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

type DefaultLogger struct {
}

func NewDefaultLogger() *DefaultLogger {
	return &DefaultLogger{}
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
	d.print(message)
}

func (d *DefaultLogger) Debugf(format string, args ...interface{}) {
	if d == nil {
		return
	}
	d.printf(format, args...)
}

func (d *DefaultLogger) Info(message string) {
	if d == nil {
		return
	}
	d.print(message)
}

func (d *DefaultLogger) Infof(format string, args ...interface{}) {
	if d == nil {
		return
	}
	d.printf(format, args...)
}

func (d *DefaultLogger) Warning(message string) {
	if d == nil {
		return
	}
	d.print(message)
}

func (d *DefaultLogger) Warningf(format string, args ...interface{}) {
	if d == nil {
		return
	}
	d.printf(format, args...)
}

func (d *DefaultLogger) Error(message string) {
	if d == nil {
		return
	}
	d.print(message)
}

func (d *DefaultLogger) Errorf(format string, args ...interface{}) {
	if d == nil {
		return
	}
	d.printf(format, args...)
}
