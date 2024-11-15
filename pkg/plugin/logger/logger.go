package logger

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"sync"
)

// Wrap the grafana logger with our own metadata.
// Ref: https://grafana.com/developers/plugin-tools/how-to-guides/data-source-plugins/add-logs-metrics-traces-for-backend-plugins

var Logger = log.New()

func Debug(msg string, args ...interface{})      { Logger.Debug(msg, args...) }
func Info(msg string, args ...interface{})       { Logger.Info(msg, args...) }
func Warn(msg string, args ...interface{})       { Logger.Warn(msg, args...) }
func Error(msg string, args ...interface{})      { Logger.Error(msg, args...) }
func With(args ...interface{}) log.Logger        { return Logger.With(args...) }
func WithError(err error) log.Logger             { return Logger.With("error", err) }
func Level() log.Level                           { return Logger.Level() }
func FromContext(ctx context.Context) log.Logger { return Logger.FromContext(ctx) }

var disableOnce sync.Once

// Disable turns offs all log messages. Only intended for testing.
func Disable() {
	disableOnce.Do(func() { Logger = log.NewNullLogger() })
}
