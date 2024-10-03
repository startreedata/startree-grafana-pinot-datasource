package test_helpers

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"github.com/stretchr/testify/require"
	"testing"
)

func SetupPinotAndCreateClient(t *testing.T) *pinotlib.PinotClient {
	pinottest.CreateTestTables(t)

	pinotClient, err := pinotlib.NewPinotClient(pinotlib.PinotClientProperties{
		ControllerUrl: pinottest.ControllerUrl,
		BrokerUrl:     pinottest.BrokerUrl,
	})
	require.NoError(t, err)
	return pinotClient
}
