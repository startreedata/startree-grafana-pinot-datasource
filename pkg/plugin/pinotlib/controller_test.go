package pinotlib

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPinotClient_ListDatabases(t *testing.T) {
	client := setupPinotAndCreateClient(t)

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := client.ListDatabases(ctx)
		assert.Contains(t, err.Error(), context.Canceled.Error())
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
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		client := setupPinotAndCreateClient(t)
		_, err := client.ListTables(ctx)
		assert.Contains(t, err.Error(), context.Canceled.Error())
	})

	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		client := setupPinotAndCreateClient(t)

		gotTables, err := client.ListTables(ctx)
		assert.NoError(t, err)
		assert.Subset(t, gotTables, []string{"infraMetrics", "githubEvents", "starbucksStores"})
	})
}

func TestPinotClient_GetTableSchema(t *testing.T) {
	client := setupPinotAndCreateClient(t)

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := client.ListTables(ctx)
		assert.Contains(t, err.Error(), context.Canceled.Error())
	})

	t.Run("githubEvents", func(t *testing.T) {
		ctx := context.Background()

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
					Format:      "TIMESTAMP",
					Granularity: "1:SECONDS",
				},
			},
		}

		got, err := client.GetTableSchema(ctx, "githubEvents")
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
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := client.GetTableMetadata(ctx, "githubEvents")
		assert.Contains(t, err.Error(), context.Canceled.Error())
	})

	t.Run("githubEvents", func(t *testing.T) {
		ctx := context.Background()

		want := TableMetadata{
			TableNameAndType: "githubEvents_OFFLINE",
		}

		got, err := client.GetTableMetadata(ctx, "githubEvents")
		assert.NoError(t, err)
		assert.Equal(t, want.TableNameAndType, got.TableNameAndType)
	})
}
