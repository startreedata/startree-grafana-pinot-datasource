package pinotlib

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"os"
	"testing"
	"time"
)

func setupPinotAndCreateClient(t *testing.T) *PinotClient {
	pinottest.CreateTestTables()

	queryRate, _ := time.ParseDuration(os.Getenv("BROKER_MAX_QUERY_RATE"))
	pinotClient := NewPinotClient(PinotClientProperties{
		ControllerUrl:      pinottest.ControllerUrl,
		BrokerUrl:          pinottest.BrokerUrl,
		BrokerCacheTimeout: time.Minute,
		//BrokerMaxConcurrentQueries: 1,
		BrokerMaxQueryRate: queryRate,
	})
	return pinotClient
}
