package test_helpers

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"testing"
)

func SetupPinotAndCreateClient(t *testing.T) *pinotlib.PinotClient {
	pinottest.CreateTestTables()
	return pinotlib.NewPinotClient(pinotlib.PinotClientProperties{
		ControllerUrl: pinottest.ControllerUrl,
		BrokerUrl:     pinottest.BrokerUrl,
	})
}
