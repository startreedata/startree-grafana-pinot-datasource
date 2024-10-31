package pinotlib

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"github.com/stretchr/testify/require"
	"testing"
)

func setupPinotAndCreateClient(t *testing.T) *PinotClient {
	pinottest.CreateTestTables()
	pinotClient, err := NewPinotClient(PinotClientProperties{
		ControllerUrl: pinottest.ControllerUrl,
		BrokerUrl:     pinottest.BrokerUrl,
	})
	require.NoError(t, err)
	return pinotClient
}
