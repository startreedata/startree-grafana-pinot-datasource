package pinotlib

import (
	"context"
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPinotClient_Query(t *testing.T) {
	ctx := context.Background()
	client := setupPinotAndCreateClient(t)
	sql := fmt.Sprintf(`select * from %s limit 10`, pinottest.InfraMetricsTableName)
	_, err := client.ExecuteSQL(ctx, pinottest.InfraMetricsTableName, sql)
	assert.NoError(t, err)
}

func setupPinotAndCreateClient(t *testing.T) *PinotClient {
	pinottest.CreateTestTables(t)

	pinotClient, err := NewPinotClient(PinotClientProperties{
		ControllerUrl: pinottest.ControllerUrl,
		BrokerUrl:     pinottest.BrokerUrl,
	})
	require.NoError(t, err)
	return pinotClient
}
