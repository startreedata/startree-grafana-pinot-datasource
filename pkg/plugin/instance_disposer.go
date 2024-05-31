package plugin

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
)

// NewInstanceDisposer here tells plugin SDK that plugin wants to clean up resources when a new instance
// created. As soon as datasource settings change detected by SDK old datasource instance will
// be disposed and a new one will be created using NewSampleDatasource factory function.
func NewInstanceDisposer() instancemgmt.InstanceDisposer {
	return disposerFunc(func() {
	})
}

type disposerFunc func()

func (f disposerFunc) Dispose() { f() }
