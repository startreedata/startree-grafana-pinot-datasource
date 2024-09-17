package pinotlib

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIsTimeSeriesTableSchema(t *testing.T) {
	t.Run("is", func(t *testing.T) {
		schema := TableSchema{
			SchemaName: "startree_metrics_analytics",
			DimensionFieldSpecs: []DimensionFieldSpec{
				{Name: "name", DataType: "STRING"},
				{Name: "labels", DataType: "JSON"},
			},
			MetricFieldSpecs: []MetricFieldSpec{
				{Name: "value", DataType: "DOUBLE"},
			},
			DateTimeFieldSpecs: []DateTimeFieldSpec{
				{Name: "timestampRoundedSeconds", DataType: "LONG"},
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
}
