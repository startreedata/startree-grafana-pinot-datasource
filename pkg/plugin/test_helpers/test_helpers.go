package test_helpers

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"os"
	"testing"
	"time"
)

func SetupPinotAndCreateClient(t *testing.T) *pinotlib.PinotClient {
	pinottest.CreateTestTables()

	queryRate, _ := time.ParseDuration(os.Getenv("BROKER_MAX_QUERY_RATE"))
	return pinotlib.NewPinotClient(pinotlib.PinotClientProperties{
		ControllerUrl:      pinottest.ControllerUrl,
		BrokerUrl:          pinottest.BrokerUrl,
		BrokerCacheTimeout: time.Minute,
		//BrokerMaxConcurrentQueries: 1,
		BrokerMaxQueryRate: queryRate,
	})
}
