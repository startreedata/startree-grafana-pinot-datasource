package pinotlib

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"testing"
)

func setupPinotAndCreateClient(t *testing.T) *PinotClient {
	pinottest.CreateTestTables()
	pinotClient := NewPinotClient(PinotClientProperties{
		ControllerUrl: pinottest.ControllerUrl,
		BrokerUrl:     pinottest.BrokerUrl,
	})
	return pinotClient
}
