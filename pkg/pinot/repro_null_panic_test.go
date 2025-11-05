package pinot

import (
	"context"
	"testing"

	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot/pinottest"
	"github.com/stretchr/testify/require"
)

// TestReproNullPanic reproduces the nil -> json.Number panic
func TestReproNullPanic(t *testing.T) {
	pinottest.CreateTestTables()
	
	client := NewPinotClient(nil, ClientProperties{
		BrokerUrl:     pinottest.BrokerUrl,
		ControllerUrl: pinottest.ControllerUrl,
	})

	// Query the nullValues table which has NULLs in numeric columns
	resp, err := client.ExecuteSqlQuery(context.Background(),
		NewSqlQuery(`SELECT __int, __long, __double, __float FROM "nullValues" ORDER BY "__timestamp" ASC`))
	
	require.NoError(t, err, "query should succeed")
	require.True(t, resp.HasData(), "should have data")

	// This will PANIC when it tries to extract the NULL values as json.Number
	// Row 0 has values, Row 1 has NULLs
	t.Log("Attempting to extract __int column with NULL values...")
	_, err = ExtractColumn(resp.ResultTable, 0) // __int column
	t.Logf("ExtractColumn error: %v", err)
}

