package pinotlib

import (
	"context"
	"errors"
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

func TestPinotClient_ListTableConfigs(t *testing.T) {
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

func TestPinotClient_CreateTableSchema(t *testing.T) {
	client := setupPinotAndCreateClient(t)

	t.Run("context cancelled", func(t *testing.T) {
		err := client.DeleteTableSchema(cancelledCtx(), "not_a_schema", true)
		assert.ErrorContains(t, err, context.Canceled.Error())
	})

	ctx := context.Background()
	require.NoError(t, client.DeleteTableSchema(ctx, "test_table", true))
	defer require.NoError(t, client.DeleteTableSchema(ctx, "test_table", true))
	assert.NoError(t, client.CreateTableSchema(ctx, TableSchema{
		SchemaName:          "test_table",
		DimensionFieldSpecs: []DimensionFieldSpec{{Name: "dim", DataType: "STRING"}},
		MetricFieldSpecs:    []MetricFieldSpec{{Name: "met", DataType: "LONG"}},
		DateTimeFieldSpecs:  []DateTimeFieldSpec{{Name: "ts", DataType: "TIMESTAMP", Format: "1:MILLISECONDS:EPOCH", Granularity: "1:MILLISECONDS"}},
	}))
}

func TestPinotClient_DeleteTableSchema(t *testing.T) {
	client := setupPinotAndCreateClient(t)
	ctx := context.Background()

	testCases := []struct {
		schemaName string
		missingOk  bool
		wantErr    error
	}{
		{schemaName: "test_table", missingOk: false, wantErr: nil},
		{schemaName: "not_a_table_missing_ok", missingOk: true, wantErr: nil},
		{schemaName: "not_a_table", missingOk: false, wantErr: errors.New("not_a_table not found")},
	}

	require.NoError(t, client.CreateTableSchema(ctx, TableSchema{
		SchemaName:          "test_table",
		DimensionFieldSpecs: []DimensionFieldSpec{{Name: "dim", DataType: "STRING"}},
		MetricFieldSpecs:    []MetricFieldSpec{{Name: "met", DataType: "LONG"}},
		DateTimeFieldSpecs:  []DateTimeFieldSpec{{Name: "ts", DataType: "TIMESTAMP", Format: "1:MILLISECONDS:EPOCH", Granularity: "1:MILLISECONDS"}},
	}))
	for _, tt := range testCases {
		t.Run(tt.schemaName, func(t *testing.T) {
			gotErr := client.DeleteTableSchema(ctx, tt.schemaName, tt.missingOk)
			if tt.wantErr != nil {
				assert.ErrorContains(t, gotErr, tt.wantErr.Error())
			} else {
				assert.NoError(t, gotErr)
			}
		})
	}

	t.Run("context cancelled", func(t *testing.T) {
		err := client.DeleteTableSchema(cancelledCtx(), "not_a_schema", true)
		assert.ErrorContains(t, err, context.Canceled.Error())
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

func TestPinotClient_TableExists(t *testing.T) {
	client := setupPinotAndCreateClient(t)
	ctx := context.Background()

	require.NoError(t, client.CreateTableSchema(ctx, TableSchema{
		SchemaName:          "test_table",
		DimensionFieldSpecs: []DimensionFieldSpec{{Name: "dim", DataType: "STRING"}},
		MetricFieldSpecs:    []MetricFieldSpec{{Name: "met", DataType: "LONG"}},
		DateTimeFieldSpecs:  []DateTimeFieldSpec{{Name: "ts", DataType: "TIMESTAMP", Format: "1:MILLISECONDS:EPOCH", Granularity: "1:MILLISECONDS"}},
	}))

	require.NoError(t, client.CreateTable(ctx, TableConfig{
		TableName: "",
		TableType: "",
		Query: struct {
			ExpressionOverrideMap map[string]string `json:"expressionOverrideMap"`
		}{},
		IngestionConfig: IngestionConfig{},
	}))

	t.Run("context cancelled", func(t *testing.T) {
		_, err := client.TableExists(cancelledCtx(), "derivedTimeBuckets")
	})
}

func TestPinotClient_CreateTable(t *testing.T) {}

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
