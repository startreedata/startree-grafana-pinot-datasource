package pinotlib

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPinotClient_ExecuteTimeSeriesQuery(t *testing.T) {
	client := NewPinotTestClient(t)

	resp, err := client.ExecuteTimeSeriesQuery(context.Background(), &TimeSeriesRangeQuery{
		Query:     "http_in_flight_requests",
		Start:     time.Unix(1727378727, 0),
		End:       time.Unix(1727379131, 0),
		Step:      1,
		TableName: "prometheusMsg_REALTIME",
	})

	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	var buf bytes.Buffer
	buf.ReadFrom(resp.Body)
	assert.Equal(t, "", buf.String())
}

func TestPinotClient_ListTimeSeriesTables(t *testing.T) {
	client := NewPinotTestClient(t)

	got, err := client.ListTimeSeriesTables(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"events"}, got)
}

func TestPinotClient_ListTimeSeriesMetrics(t *testing.T)     {}
func TestPinotClient_ListTimeSeriesLabels(t *testing.T)      {}
func TestPinotClient_ListTimeSeriesLabelValues(t *testing.T) {}

func TestPinotClient_IsTimeSeriesTable(t *testing.T) {
	t.Run("is", func(t *testing.T) {
		client := NewPinotTestClient(t)
		got, err := client.IsTimeSeriesTable(context.Background(), "prometheusMsg")
		assert.NoError(t, err)
		assert.True(t, got)
	})

	t.Run("isnt", func(t *testing.T) {
		client := NewPinotTestClient(t)
		got, err := client.IsTimeSeriesTable(context.Background(), "somethingelse")
		assert.NoError(t, err)
		assert.True(t, got)
	})
}

func TestIsTimeSeriesTableSchema(t *testing.T) {
	t.Run("is", func(t *testing.T) {
		schema := TableSchema{
			SchemaName: "startree_metrics_analytics",
			DimensionFieldSpecs: []DimensionFieldSpec{
				{Name: "metric", DataType: "STRING"},
				{Name: "labels", DataType: "JSON"},
			},
			MetricFieldSpecs: []MetricFieldSpec{
				{Name: "value", DataType: "DOUBLE"},
			},
			DateTimeFieldSpecs: []DateTimeFieldSpec{
				{Name: "ts", DataType: "TIMESTAMP"},
			},
		}

		assert.Equal(t, true, IsTimeSeriesTableSchema(schema))
	})

	t.Run("isnt", func(t *testing.T) {
		schema := TableSchema{
			SchemaName:          "startree_metrics_analytics",
			DimensionFieldSpecs: nil,
			MetricFieldSpecs:    nil,
			DateTimeFieldSpecs:  nil,
		}
		assert.Equal(t, false, IsTimeSeriesTableSchema(schema))
	})
}
