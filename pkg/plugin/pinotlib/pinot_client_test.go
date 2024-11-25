package pinotlib

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"testing"
	"time"
)

func setupPinotAndCreateClient(t *testing.T) *PinotClient {
	pinottest.CreateTestTables()
	pinotClient := NewPinotClient(PinotClientProperties{
		ControllerUrl:      pinottest.ControllerUrl,
		BrokerUrl:          pinottest.BrokerUrl,
		BrokerCacheTimeout: time.Minute,
		//BrokerMaxConcurrentQueries: 1,
		//BrokerMaxQueryRate:         1 * time.Second,
	})
	return pinotClient
}
