package log

type Interface interface {
	Level() Level
	SetLevel(l Level)
	Error(format string, v ...interface{})
	Warn(format string, v ...interface{})
	Info(format string, v ...interface{})
	Debug(format string, v ...interface{})
}

type Level int

const (
	Error Level = iota
	Warn
	Info
	Debug
)
