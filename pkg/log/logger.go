package log

import (
	"fmt"
	"os"
	"strings"
)

type (
	Logger interface {
		Debugf(msg string, args ...any)
		Infof(msg string, args ...any)
		Errorf(msg string, args ...any)
		Warnf(msg string, args ...any)
	}

	simpleLogger struct {
		verbose bool
	}
)

// SimpleLogger is a bare-bones implementation of the logging interface, e.g., used for testing.
// It can take an optional verbose flag. If verbose is false (or not provided), Debugf and Infof will be no-ops.
func SimpleLogger(verbose ...bool) Logger {
	isVerbose := false
	if len(verbose) > 0 {
		isVerbose = verbose[0]
	}
	return &simpleLogger{verbose: isVerbose}
}

func (l *simpleLogger) Errorf(msg string, args ...any) {
	logWithLevel("ERROR", msg, args...)
}

func (l *simpleLogger) Warnf(msg string, args ...any) {
	logWithLevel("WARNING", msg, args...)
}

func (l *simpleLogger) Debugf(msg string, args ...any) {
	if !l.verbose {
		return
	}
	logWithLevel("DEBUG", msg, args...)
}

func (l *simpleLogger) Infof(msg string, args ...any) {
	if !l.verbose {
		return
	}
	logWithLevel("INFO", msg, args...)
}

func logWithLevel(level, format string, args ...interface{}) {
	paddedLevel := strings.ToUpper(level)
	for len(paddedLevel) < 7 {
		paddedLevel += " "
	}
	// Add a newline to the end of the log message for better formatting.
	fmt.Fprintf(os.Stderr, "[%s] %s\n", paddedLevel, fmt.Sprintf(format, args...))
}
