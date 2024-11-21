package dataquery

import (
	"context"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPinotVariableQueryDriver_Execute(t *testing.T) {
	client := test_helpers.SetupPinotAndCreateClient(t)

	// TODO: Add tests for error cases

	t.Run("variableType="+VariableQueryTypeTableList, func(t *testing.T) {
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
				assert.Subset(t, gotTable, []string{"infraMetrics", "benchmark", "partial"}, "DataResponse.Frames[0].Fields[0]")
			}

			assert.Empty(t, got.ErrorSource, "DataResponse.ErrorSource")
			assert.NoError(t, got.Error, "DataResponse.Error")
		})
	})
	t.Run("variableType="+VariableQueryTypeColumnList, func(t *testing.T) {
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

	t.Run("variableType="+VariableQueryTypeDistinctValues, func(t *testing.T) {
		newDriver := func(testCase DriverTestCase) (Driver, error) {
			return NewPinotVariableQueryDriver(PinotVariableQueryParams{
				PinotClient:  testCase.Client,
				VariableType: VariableQueryTypeDistinctValues,
				TableName:    testCase.TableName,
				ColumnName:   testCase.TargetColumn,
			}), nil
		}

		wantFrames := func(values []string) data.Frames {
			return data.Frames{data.NewFrame("result",
				data.NewField("distinctValues", nil, values),
			)}
		}

		t.Run("happy path", func(t *testing.T) {
			runSqlQueryDistinctValsHappyPath(t, newDriver, wantFrames)
		})
		t.Run("partial data", func(t *testing.T) {
			runSqlQueryDistinctValsPartialResults(t, newDriver, wantFrames)
		})
		t.Run("no rows", func(t *testing.T) {
			runSqlQueryNoRows(t, newDriver)
		})
		t.Run("column dne", func(t *testing.T) {
			runSqlQueryColumnDne(t, newDriver)
		})
		t.Run("pinot unreachable", func(t *testing.T) {
			runSqlQueryPinotUnreachable(t, newDriver)
		})
	})

	t.Run("variableType="+VariableQueryTypePinotQlCode, func(t *testing.T) {
		newDriver := func(testCase DriverTestCase) (Driver, error) {
			return NewPinotVariableQueryDriver(PinotVariableQueryParams{
				PinotClient:  testCase.Client,
				VariableType: VariableQueryTypePinotQlCode,
				TableName:    testCase.TableName,
				PinotQlCode: fmt.Sprintf(`SELECT DISTINCT "%s"
FROM "%s"
ORDER BY "%s" ASC
LIMIT 100;`, testCase.TargetColumn, testCase.TableName, testCase.TargetColumn),
			}), nil
		}

		wantFrames := func(values []string) data.Frames {
			return data.Frames{data.NewFrame("result",
				data.NewField("codeValues", nil, values),
			)}
		}

		t.Run("happy path", func(t *testing.T) {
			runSqlQueryDistinctValsHappyPath(t, newDriver, wantFrames)
		})
		t.Run("partial data", func(t *testing.T) {
			runSqlQueryDistinctValsPartialResults(t, newDriver, wantFrames)
		})
		t.Run("no rows", func(t *testing.T) {
			runSqlQueryNoRows(t, newDriver)
		})
		t.Run("column dne", func(t *testing.T) {
			runSqlQueryColumnDne(t, newDriver)
		})
		t.Run("pinot unreachable", func(t *testing.T) {
			runSqlQueryPinotUnreachable(t, newDriver)
		})
	})
}
