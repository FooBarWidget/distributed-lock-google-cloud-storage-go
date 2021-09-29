package log

import (
	"fmt"
	"io"
	"os"
)

type Logger struct {
	output io.Writer
	level  Level
}

var (
	_ Interface = &Logger{} // ensure Logger implements Interface

	Default = NewLogger(os.Stderr)
)

func NewLogger(out io.Writer) Logger {
	return Logger{output: out, level: Info}
}

func (l Logger) Level() Level {
	return l.level
}

func (l *Logger) SetLevel(v Level) {
	l.level = v
}

func (l Logger) Error(format string, v ...interface{}) {
	if l.level >= Error {
		fmt.Fprintf(l.output, "E: "+format+"\n", v...)
	}
}

func (l Logger) Warn(format string, v ...interface{}) {
	if l.level >= Warn {
		fmt.Fprintf(l.output, "W: "+format+"\n", v...)
	}
}

func (l Logger) Info(format string, v ...interface{}) {
	if l.level >= Info {
		fmt.Fprintf(l.output, "I: "+format+"\n", v...)
	}
}

func (l Logger) Debug(format string, v ...interface{}) {
	if l.level >= Debug {
		fmt.Fprintf(l.output, "D: "+format+"\n", v...)
	}
}
