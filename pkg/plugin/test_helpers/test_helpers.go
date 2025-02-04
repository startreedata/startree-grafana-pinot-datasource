package test_helpers

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"net/http"
	"testing"
)

func SetupPinotAndCreateClient(t *testing.T) *pinotlib.PinotClient {
	pinottest.CreateTestTables()
	return pinotlib.NewPinotClient(http.DefaultClient, pinotlib.PinotClientProperties{
		ControllerUrl: pinottest.ControllerUrl,
		BrokerUrl:     pinottest.BrokerUrl,
	})
}
