package pinotlib

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

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
				{Name: "ts", DataType: "LONG"},
			},
		}

		assert.Equal(t, true, IsTimeSeriesTableSchema(schema))
	})

	t.Run("isnt", func(t *testing.T) {
		schema := TableSchema{
			SchemaName:          "",
			DimensionFieldSpecs: nil,
			MetricFieldSpecs:    nil,
			DateTimeFieldSpecs:  nil,
		}

		assert.Equal(t, false, IsTimeSeriesTableSchema(schema))
	})

	t.Run("x", func(t *testing.T) {
		client := NewPinotTestClient(t)

		got, err := client.ListTimeSeriesTables(context.Background())
		require.NoError(t, err)
		assert.Equal(t, []string{"events"}, got)
	})
}
