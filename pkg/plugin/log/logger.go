package log

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"sync"
)

// Wrap the grafana logger with our own metadata.
// Ref: https://grafana.com/developers/plugin-tools/how-to-guides/data-source-plugins/add-logs-metrics-traces-for-backend-plugins

type Logger = log.Logger

var logger = log.New()

func Debug(msg string, args ...interface{})      { logger.Debug(msg, args...) }
func Info(msg string, args ...interface{})       { logger.Info(msg, args...) }
func Warn(msg string, args ...interface{})       { logger.Warn(msg, args...) }
func Error(msg string, args ...interface{})      { logger.Error(msg, args...) }
func With(args ...interface{}) log.Logger        { return logger.With(args...) }
func WithError(err error) log.Logger             { return logger.With("error", err) }
func Level() log.Level                           { return logger.Level() }
func FromContext(ctx context.Context) log.Logger { return logger.FromContext(ctx) }

var disableOnce sync.Once

// Disable turns offs all log messages. Only intended for testing.
func Disable() {
	disableOnce.Do(func() { logger = log.NewNullLogger() })
}
