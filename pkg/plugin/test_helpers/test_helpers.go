package test_helpers

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

var setupOnce sync.Once
var pinotClient *pinotlib.PinotClient

func SetupPinotAndCreateClient(t *testing.T) *pinotlib.PinotClient {
	setupOnce.Do(func() {
		pinottest.CreateTestTables()

		var err error
		pinotClient, err = pinotlib.NewPinotClient(pinotlib.PinotClientProperties{
			ControllerUrl: pinottest.ControllerUrl,
			BrokerUrl:     pinottest.BrokerUrl,
		})
		require.NoError(t, err)
	})
	return pinotClient
}
