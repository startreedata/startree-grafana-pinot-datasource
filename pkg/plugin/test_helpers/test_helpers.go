package test_helpers

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot/pinottest"
	"net/http"
	"testing"
)

func SetupPinotAndCreateClient(t *testing.T) *pinot.Client {
	pinottest.CreateTestTables()
	return pinot.NewPinotClient(http.DefaultClient, pinot.ClientProperties{
		ControllerUrl: pinottest.ControllerUrl,
		BrokerUrl:     pinottest.BrokerUrl,
	})
}
