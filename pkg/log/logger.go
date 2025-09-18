package log

import (
	"fmt"
	"os"
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
		debug   bool
	}
)

// SimpleLogger is a bare-bones implementation of the logging interface, e.g., used for testing
func SimpleLogger(verbose bool, debug bool) Logger {
	return &simpleLogger{verbose: verbose, debug: debug}
}

func (s *simpleLogger) Debugf(msg string, args ...any) {
	if s.verbose && s.debug {
		s.logWithLevel("DEBUG", msg, args...)
	}
}

func (s *simpleLogger) Infof(msg string, args ...any) {
	if s.verbose {
		s.logWithLevel("INFO", msg, args...)
	}
}

func (s *simpleLogger) Errorf(msg string, args ...any) {
	s.logWithLevel("ERROR", msg, args...)
}

func (s *simpleLogger) Warnf(msg string, args ...any) {
	s.logWithLevel("WARNING", msg, args...)
}

func (s *simpleLogger) logWithLevel(level, msg string, args ...any) {
	formattedMessage := fmt.Sprintf(msg, args...)
	_, _ = fmt.Fprintf(os.Stderr, "[%s] %s\n", level, formattedMessage)
}