package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPinotVariableQueryDriver_Execute(t *testing.T) {
	client := test_helpers.SetupPinotAndCreateClient(t)

	// TODO: Add tests for error cases

	t.Run(VariableQueryTypeTableList, func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			params := PinotVariableQueryParams{
				PinotClient:  client,
				VariableType: VariableQueryTypeTableList,
			}

			driver := NewPinotVariableQueryDriver(params)
			got := driver.Execute(context.Background())

			assert.Equal(t, backend.StatusOK, got.Status, "DataResponse.Status")
			if assert.Len(t, got.Frames, 1, "DataResponse.Frames") && assert.Len(t, got.Frames[0].Fields, 1, "DataResponse.Frames[0].Fields") {
				field := got.Frames[0].Fields[0]
				var gotTable []string
				for i := 0; i < field.Len(); i++ {
					gotTable = append(gotTable, field.At(i).(string))
				}
				assert.Subset(t, gotTable, []string{"infraMetrics", "githubEvents", "starbucksStores"}, "DataResponse.Frames[0].Fields[0]")
			}

			assert.Empty(t, got.ErrorSource, "DataResponse.ErrorSource")
			assert.NoError(t, got.Error, "DataResponse.Error")
		})
	})
	t.Run(VariableQueryTypeColumnList, func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			params := PinotVariableQueryParams{
				PinotClient:  client,
				VariableType: VariableQueryTypeColumnList,
				TableName:    "benchmark",
			}

			driver := NewPinotVariableQueryDriver(params)
			got := driver.Execute(context.Background())

			assert.Equal(t, backend.StatusOK, got.Status, "DataResponse.Status")
			assert.Equal(t, data.Frames{data.NewFrame("result",
				data.NewField("columns", nil, []string{"ts", "value", "fabric", "pattern"}),
			)}, got.Frames, "DataResponse.Frames")

			assert.Empty(t, got.ErrorSource, "DataResponse.ErrorSource")
			assert.NoError(t, got.Error, "DataResponse.Error")
		})
	})
	t.Run(VariableQueryTypeDistinctValues, func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			params := PinotVariableQueryParams{
				PinotClient:  client,
				VariableType: VariableQueryTypeDistinctValues,
				TableName:    "infraMetrics",
				ColumnName:   "metric",
			}

			driver := NewPinotVariableQueryDriver(params)
			got := driver.Execute(context.Background())

			assert.Equal(t, backend.StatusOK, got.Status, "DataResponse.Status")
			assert.Equal(t, data.Frames{data.NewFrame("result",
				data.NewField("distinctValues", nil, []string{"db_record_write", "http_request_handled"}),
			)}, got.Frames, "DataResponse.Frames")

			assert.Empty(t, got.ErrorSource, "DataResponse.ErrorSource")
			assert.NoError(t, got.Error, "DataResponse.Error")
		})
	})
	t.Run(VariableQueryTypePinotQlCode, func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			params := PinotVariableQueryParams{
				PinotClient:  client,
				VariableType: VariableQueryTypePinotQlCode,
				TableName:    "infraMetrics",
				PinotQlCode:  `select distinct metric from infraMetrics`,
			}

			driver := NewPinotVariableQueryDriver(params)
			got := driver.Execute(context.Background())

			assert.Equal(t, backend.StatusOK, got.Status, "DataResponse.Status")
			assert.Equal(t, data.Frames{data.NewFrame("result",
				data.NewField("codeValues", nil, []string{"db_record_write", "http_request_handled"}),
			)}, got.Frames, "DataResponse.Frames")

			assert.Empty(t, got.ErrorSource, "DataResponse.ErrorSource")
			assert.NoError(t, got.Error, "DataResponse.Error")
		})
	})
}
