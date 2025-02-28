package pinot

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPinotClient_ListDatabases(t *testing.T) {
	client := setupPinotAndCreateClient(t)

	t.Run("context cancelled", func(t *testing.T) {
		_, err := client.ListDatabases(cancelledCtx())
		assert.ErrorContains(t, err, context.Canceled.Error())
	})

	t.Run("success", func(t *testing.T) {
		ctx := context.Background()

		got, err := client.ListDatabases(ctx)
		assert.NoError(t, err)
		assert.Equal(t, got, []string{"default"})
	})
}

func TestPinotClient_ListTables(t *testing.T) {
	t.Run("context cancelled", func(t *testing.T) {
		client := setupPinotAndCreateClient(t)
		_, err := client.ListTables(cancelledCtx())
		assert.ErrorContains(t, err, context.Canceled.Error())
	})

	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		client := setupPinotAndCreateClient(t)

		gotTables, err := client.ListTables(ctx)
		assert.NoError(t, err)
		assert.Subset(t, gotTables, []string{"infraMetrics", "benchmark", "partial"})
	})
}

func TestPinotClient_GetTableSchema(t *testing.T) {
	client := setupPinotAndCreateClient(t)

	t.Run("context cancelled", func(t *testing.T) {
		_, err := client.GetTableSchema(cancelledCtx(), "benchmark")
		assert.ErrorContains(t, err, context.Canceled.Error())
	})

	t.Run("benchmark", func(t *testing.T) {
		ctx := context.Background()

		want := TableSchema{
			SchemaName: "benchmark",
			DimensionFieldSpecs: []DimensionFieldSpec{
				{Name: "fabric", DataType: "STRING"},
				{Name: "pattern", DataType: "STRING"},
			},
			MetricFieldSpecs: []MetricFieldSpec{{
				Name:     "value",
				DataType: "DOUBLE",
			}},
			DateTimeFieldSpecs: []DateTimeFieldSpec{{
				Name:        "ts",
				DataType:    "TIMESTAMP",
				Format:      "TIMESTAMP",
				Granularity: "1:MILLISECONDS",
			}},
		}

		got, err := client.GetTableSchema(ctx, "benchmark")
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})
}

func TestPinotClient_GetTableConfig(t *testing.T) {
	client := setupPinotAndCreateClient(t)
	config, err := client.ListTableConfigs(context.Background(), "derivedTimeBuckets")
	require.NoError(t, err)
	require.Equal(t, ListTableConfigsResponse{
		"OFFLINE": TableConfig{
			TableName: "derivedTimeBuckets_OFFLINE",
			TableType: "OFFLINE",
			IngestionConfig: IngestionConfig{
				TransformConfigs: []TransformConfig{
					{ColumnName: "ts_1m", TransformFunction: "FromEpochMinutesBucket(ToEpochMinutesBucket(\"ts\", 1), 1)"},
					{ColumnName: "ts_2m", TransformFunction: "FromEpochMinutesBucket(ToEpochMinutesBucket(\"ts\", 2), 2)"},
					{ColumnName: "ts_5m", TransformFunction: "FromEpochMinutesBucket(ToEpochMinutesBucket(\"ts\", 5), 5)"},
					{ColumnName: "ts_10m", TransformFunction: "FromEpochMinutesBucket(ToEpochMinutesBucket(\"ts\", 10), 10)"},
					{ColumnName: "ts_15m", TransformFunction: "FromEpochMinutesBucket(ToEpochMinutesBucket(\"ts\", 15), 15)"},
					{ColumnName: "ts_30m", TransformFunction: "FromEpochMinutesBucket(ToEpochMinutesBucket(\"ts\", 30), 30)"},
					{ColumnName: "ts_1h", TransformFunction: "FromEpochHoursBucket(ToEpochHoursBucket(\"ts\", 1), 1)"},
					{ColumnName: "ts_1d", TransformFunction: "FromEpochDaysBucket(ToEpochDaysBucket(\"ts\", 1), 1)"},
				},
			},
		},
	}, config)
}

func TestPinotClient_GetTableMetadata(t *testing.T) {
	client := setupPinotAndCreateClient(t)

	t.Run("context cancelled", func(t *testing.T) {
		_, err := client.GetTableMetadata(cancelledCtx(), "benchmark")
		assert.ErrorContains(t, err, context.Canceled.Error())
	})

	t.Run("benchmark", func(t *testing.T) {
		ctx := context.Background()

		want := TableMetadata{
			TableNameAndType: "benchmark_OFFLINE",
		}

		got, err := client.GetTableMetadata(ctx, "benchmark")
		assert.NoError(t, err)
		assert.Equal(t, want.TableNameAndType, got.TableNameAndType)
	})
}

func cancelledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	<-ctx.Done()
	return ctx
}
