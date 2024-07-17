package plugin

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
)

func TestPinotClient_Query(t *testing.T) {
	ctx := context.Background()
	client := newPinotTestClient(t)
	sql := `select * from githubEvents limit 10`
	_, err := client.ExecuteSQL(ctx, "githubEvents", sql)
	assert.NoError(t, err)
}

func TestPinotClient_ListTables(t *testing.T) {
	ctx := context.Background()
	client := newPinotTestClient(t)

	wantTables := []string{"airlineStats", "baseballStats", "billing",
		"dimBaseballTeams", "githubComplexTypeEvents", "githubEvents", "starbucksStores"}

	gotTables, err := client.ListTables(ctx, "")
	sort.Strings(gotTables)

	assert.NoError(t, err)
	assert.EqualValues(t, wantTables, gotTables)
}

func TestPinotClient_GetTableSchema(t *testing.T) {
	ctx := context.Background()
	client := newPinotTestClient(t)

	want := TableSchema{
		SchemaName: "githubEvents",
		DimensionFieldSpecs: []DimensionFieldSpec{
			{Name: "id", DataType: "STRING"},
			{Name: "type", DataType: "STRING"},
			{Name: "actor", DataType: "JSON"},
			{Name: "repo", DataType: "JSON"},
			{Name: "payload", DataType: "JSON"},
			{Name: "public", DataType: "BOOLEAN"},
		},
		MetricFieldSpecs: nil,
		DateTimeFieldSpecs: []DateTimeFieldSpec{
			{
				Name:        "created_at",
				DataType:    "STRING",
				Format:      "1:SECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ss'Z'",
				Granularity: "1:SECONDS",
			},
			{
				Name:        "created_at_timestamp",
				DataType:    "TIMESTAMP",
				Format:      "1:MILLISECONDS:TIMESTAMP",
				Granularity: "1:SECONDS",
			},
		},
	}

	got, err := client.GetTableSchema(ctx, "", "githubEvents")
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func newPinotTestClient(t *testing.T) *PinotClient {
	pinotClient, err := NewPinotClient(PinotClientProperties{
		ControllerUrl: "http://localhost:9000",
		BrokerUrl:     "http://localhost:8000",
	})
	require.NoError(t, err)
	return pinotClient
}
