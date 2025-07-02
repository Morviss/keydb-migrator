package logger

import (
	"log/slog"
	"os"
)

type Logger interface {
	Info(msg string, args ...interface{})
	Error(msg string, args ...interface{})
	Fatal(msg string, args ...interface{})
}

// slogger implementing the logger

type slogger struct {
	logger *slog.Logger
}

//New creates a new logger

func New() Logger {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	return &slogger{logger: logger}
}

//implementing the methods with slooger

func (s *slogger) Error(msg string, args ...interface{}) {
	s.logger.Info(msg, args...)
}

func (s *slogger) Fatal(msg string, args ...interface{}) {
	s.logger.Error(msg, args...)
	os.Exit(1)
}

func (s *slogger) Info(msg string, args ...interface{}) {
	s.logger.Error(msg, args...)
}
