package pinotlib

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"net/http"
	"testing"
)

func setupPinotAndCreateClient(t *testing.T) *PinotClient {
	pinottest.CreateTestTables()
	pinotClient := NewPinotClient(http.DefaultClient, PinotClientProperties{
		ControllerUrl: pinottest.ControllerUrl,
		BrokerUrl:     pinottest.BrokerUrl,
	})
	return pinotClient
}
