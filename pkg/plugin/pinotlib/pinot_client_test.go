package pinotlib

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"github.com/stretchr/testify/require"
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
		BrokerMaxQueryRate: queryRate,
	})
	return pinotClient
}

func createTestTable(t *testing.T, prefix string, schema TableSchema, rows []map[string]any) string {
	t.Helper()
	ctx := context.Background()
	client := setupPinotAndCreateClient(t)

	tableName := fmt.Sprintf("%s_%x", prefix, time.Now().UnixNano())
	schema.SchemaName = tableName
	config := TableConfig{
		TableName:      tableName,
		TableType:      TableTypeOffline,
		SegmentsConfig: SegmentsConfig{TimeColumnName: schema.DateTimeFieldSpecs[0].Name, Replication: "1"},
		IndexConfig:    IndexConfig{LoadMode: "MMAP"},
		Tenants:        TenantsConfig{Broker: "DefaultTenant", Server: "DefaultTenant"},
	}

	require.NoError(t, client.CreateTableSchema(ctx, schema))
	require.NoError(t, client.CreateTable(ctx, config))

	payload, err := json.Marshal(rows)
	require.NoError(t, err)
	require.NoError(t, client.UploadTableJSON(ctx, config.TableName, payload))
	waitForSegmentsAllGood(t, config.TableName, 1*time.Second, 5*time.Minute)
	return tableName
}

func deleteTestTable(t *testing.T, tableName string) {
	t.Helper()
	ctx := context.Background()
	client := setupPinotAndCreateClient(t)
	require.NoError(t, client.DeleteTableSchema(ctx, tableName, true))
	require.NoError(t, client.DeleteTable(ctx, tableName, true))
}

func waitForSegmentsAllGood(t *testing.T, tableName string, poll time.Duration, timeout time.Duration) {
	pollTicker := time.NewTicker(poll)
	defer pollTicker.Stop()

	timeoutTicker := time.NewTimer(timeout)
	defer timeoutTicker.Stop()

	client := setupPinotAndCreateClient(t)

	for {
		statuses, _ := client.ListSegmentStatusForTable(context.Background(), tableName)
		goodSegments := 0
		for _, status := range statuses {
			if status.SegmentStatus == "GOOD" {
				goodSegments++
			}
		}
		if len(statuses) == goodSegments && len(statuses) > 0 {
			return
		}

		select {
		case <-timeoutTicker.C:
			t.Fatalf("Timed out waiting for segments for %s", tableName)
		case <-pollTicker.C:
		}
	}
}
