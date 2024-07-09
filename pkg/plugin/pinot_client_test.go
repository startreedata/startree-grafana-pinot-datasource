package plugin

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPinotClient_Query(t *testing.T) {
	ctx := context.Background()
	client := newPinotClient(t)

	sql := `SELECT "timestamp" AS 'time' FROM LogAnalyticsMonitoringwhere "timestamp" >= 1715881482102 AND "timestamp" <= 1715859882102`

	_, err := client.ExecuteSQL(ctx, "LogAnalyticsMonitoring", sql)
	assert.NoError(t, err)
}

func TestPinotClient_ListTables(t *testing.T) {
	ctx := context.Background()
	client := newPinotClient(t)

	gotTables, err := client.ListTables(ctx, "")

	assert.NoError(t, err)
	assert.NotEmpty(t, gotTables)
}

func TestPinotClient_GetTableSchema(t *testing.T) {
	ctx := context.Background()
	client := newPinotClient(t)

	res, err := client.GetTableSchema(ctx, "", "ABTestSampleData")

	assert.NoError(t, err)
	fmt.Printf("%v", res)
	assert.NotEmpty(t, res)
}

func newPinotClient(t *testing.T) *PinotClient {
	pinotClient, err := NewPinotClient(PinotClientProperties{
		ControllerUrl: "https://pinot.demo.teprod.startree.cloud",
		BrokerUrl:     "https://broker.pinot.demo.teprod.startree.cloud",
		Authorization: "Basic YjBmZWI0YjcxN2UyNGE4M2E4NTE2OGRlMWMzODY3ODM6dnM3TkhjWjYrRTVFSXZ3OUpma0ZETnFtZmYrOTFZUk5NbHN1WkZucVVrMD0=",
	})
	require.NoError(t, err)
	return pinotClient
}
