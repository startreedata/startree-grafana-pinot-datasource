package test_helpers

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/stretchr/testify/require"
	"testing"
)

func NewPinotTestClient(t *testing.T) *pinotlib.PinotClient {
	pinotClient, err := pinotlib.NewPinotClient(pinotlib.PinotClientProperties{
		ControllerUrl: "http://localhost:9000",
		BrokerUrl:     "http://localhost:8000",
	})
	require.NoError(t, err)
	return pinotClient
}
