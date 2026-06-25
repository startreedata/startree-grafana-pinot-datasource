package dataquery

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPinotQlCodeDriver_Execute(t *testing.T) {
	t.Run("displayType=TIMESERIES", func(t *testing.T) {
		newDriver := func(testCase DriverTestCase) ExecutableQuery {
			return PinotQlCodeQuery{
				TableName:         testCase.TableName,
				TimeRange:         testCase.TimeRange,
				IntervalSize:      testCase.IntervalSize,
				DisplayType:       DisplayTypeTimeSeries,
				MetricColumnAlias: "value",
				TimeColumnAlias:   "time",
				Legend:            "test-legend",
				Code: fmt.Sprintf(`SELECT
    $__timeGroup("%s") AS $__timeAlias(),
    SUM("%s") AS $__metricAlias()
FROM
    $__table()
WHERE
    $__timeFilter("%s")
GROUP BY
    $__timeGroup("%s")
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;`, testCase.TimeColumn, testCase.TargetColumn, testCase.TimeColumn, testCase.TimeColumn),
			}
		}

		wantFrames := func(times []time.Time, values []float64) data.Frames {
			return data.Frames{data.NewFrame("response",
				data.NewField("value", data.Labels{}, sliceToPointers(values)).SetConfig(&data.FieldConfig{DisplayNameFromDS: "test-legend"}),
				data.NewField("time", nil, times),
			)}
		}

		t.Run("happy path", func(t *testing.T) {
			runSqlQuerySumHappyPath(t, newDriver, wantFrames)
		})
		t.Run("partial data", func(t *testing.T) {
			runSqlQuerySumPartialResults(t, newDriver, wantFrames)
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

	t.Run("display=TABLE", func(t *testing.T) {
		newDriver := func(testCase DriverTestCase) ExecutableQuery {
			return PinotQlCodeQuery{
				TableName:         testCase.TableName,
				TimeRange:         testCase.TimeRange,
				IntervalSize:      testCase.IntervalSize,
				DisplayType:       DisplayTypeTable,
				MetricColumnAlias: "value",
				TimeColumnAlias:   "time",
				Legend:            "test-legend",
				Code: fmt.Sprintf(`SELECT
    $__timeGroup("%s") AS $__timeAlias(),
    SUM("%s") AS $__metricAlias()
FROM
    $__table()
WHERE
    $__timeFilter("%s")
GROUP BY
    $__timeGroup("%s")
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;`, testCase.TimeColumn, testCase.TargetColumn, testCase.TimeColumn, testCase.TimeColumn),
			}
		}

		wantFrames := func(times []time.Time, values []float64) data.Frames {
			return data.Frames{data.NewFrame("response",
				data.NewField("time", nil, times),
				data.NewField("value", nil, values),
			)}
		}

		t.Run("happy path", func(t *testing.T) {
			runSqlQuerySumHappyPath(t, newDriver, wantFrames)
		})
		t.Run("partial data", func(t *testing.T) {
			runSqlQuerySumPartialResults(t, newDriver, wantFrames)
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

	t.Run("display=ANNOTATIONS", func(t *testing.T) {
		newDriver := func(testCase DriverTestCase) ExecutableQuery {
			return PinotQlCodeQuery{
				TableName:         testCase.TableName,
				TimeRange:         testCase.TimeRange,
				IntervalSize:      testCase.IntervalSize,
				DisplayType:       DisplayTypeAnnotations,
				MetricColumnAlias: "value",
				TimeColumnAlias:   "time",
				Legend:            "test-legend",
				Code: fmt.Sprintf(`SELECT
    $__timeGroup("%s") AS $__timeAlias(),
    SUM("%s") AS $__metricAlias()
FROM
    $__table()
WHERE
    $__timeFilter("%s")
GROUP BY
    $__timeGroup("%s")
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;`, testCase.TimeColumn, testCase.TargetColumn, testCase.TimeColumn, testCase.TimeColumn),
			}
		}

		wantFrames := func(times []time.Time, values []float64) data.Frames {
			return data.Frames{data.NewFrame("response",
				data.NewField("time", nil, times),
				data.NewField("value", nil, values),
			)}
		}

		t.Run("happy path", func(t *testing.T) {
			runSqlQuerySumHappyPath(t, newDriver, wantFrames)
		})
		t.Run("partial data", func(t *testing.T) {
			runSqlQuerySumPartialResults(t, newDriver, wantFrames)
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

		// Grafana maps result columns to annotation events by name and renders a region
		// (start -> end) when the frame exposes both a `time` and a `timeEnd` field. The table
		// extractor passes any aliased column straight through, so selecting a `timeEnd` column
		// yields a region annotation. Start and end share a bucket here to keep the fixture
		// deterministic; what matters is that `timeEnd` is carried through as an epoch-millis field.
		t.Run("region", func(t *testing.T) {
			client := test_helpers.SetupPinotAndCreateClient(t)

			query := PinotQlCodeQuery{
				TableName:         "benchmark",
				TimeColumnAlias:   "time",
				MetricColumnAlias: "value",
				DisplayType:       DisplayTypeAnnotations,
				IntervalSize:      1 * time.Minute,
				TimeRange: TimeRange{
					From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
					To:   time.Date(2024, 10, 1, 0, 5, 0, 0, time.UTC),
				},
				Code: `SELECT
    $__timeGroup("ts") AS $__timeAlias(),
    $__timeGroup("ts") AS "timeEnd",
    SUM("value") AS $__metricAlias()
FROM
    $__table()
WHERE
    $__timeFilter("ts")
GROUP BY
    $__timeGroup("ts")
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;`,
			}

			got := query.Execute(client, context.Background())
			assert.Equal(t, backend.StatusOK, got.Status, "DataResponse.Status")
			require.NoError(t, got.Error, "DataResponse.Error")

			starts := []time.Time{
				time.Date(2024, 10, 1, 0, 4, 0, 0, time.UTC),
				time.Date(2024, 10, 1, 0, 3, 0, 0, time.UTC),
				time.Date(2024, 10, 1, 0, 2, 0, 0, time.UTC),
				time.Date(2024, 10, 1, 0, 1, 0, 0, time.UTC),
				time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			}
			ends := make([]int64, len(starts))
			for i := range starts {
				ends[i] = starts[i].UnixMilli()
			}

			wantFrame := data.NewFrame("response",
				data.NewField("time", nil, starts),
				data.NewField("timeEnd", nil, ends),
				data.NewField("value", nil, []float64{
					4.995000894259197e+07,
					4.9950041761314005e+07,
					4.9949916961369045e+07,
					4.994997804782016e+07,
					4.995001567005852e+07,
				}),
			)
			assert.Equal(t, data.Frames{wantFrame}, got.Frames, "DataResponse.Frames")
			assert.Empty(t, got.ErrorSource, "DataResponse.ErrorSource")
		})
	})

	t.Run("displayType=LOGS", func(t *testing.T) {
		newDriver := func(testCase DriverTestCase) ExecutableQuery {
			return PinotQlCodeQuery{
				TableName:       testCase.TableName,
				TimeRange:       testCase.TimeRange,
				IntervalSize:    testCase.IntervalSize,
				DisplayType:     DisplayTypeLogs,
				LogColumnAlias:  "message",
				TimeColumnAlias: "time",
				Legend:          "test-legend",
				Code: fmt.Sprintf(`SELECT
    $__timeGroup("%s") AS $__timeAlias(),
    SUM("%s") AS "message"
FROM
    $__table()
WHERE
    $__timeFilter("%s")
GROUP BY
    $__timeGroup("%s")
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;`, testCase.TimeColumn, testCase.TargetColumn, testCase.TimeColumn, testCase.TimeColumn),
			}
		}

		wantFrames := func(times []time.Time, values []float64) data.Frames {
			labels := make([]json.RawMessage, len(times))
			for i := range labels {
				labels[i] = json.RawMessage(`{}`)
			}

			frame := data.NewFrame("response",
				data.NewField("labels", nil, labels),
				data.NewField("Line", nil, sliceToStrings(values)),
				data.NewField("Time", nil, times))
			frame.Meta = &data.FrameMeta{
				Custom: map[string]interface{}{"frameType": "LabeledTimeValues"},
			}

			return data.Frames{frame}
		}

		t.Run("happy path", func(t *testing.T) {
			runSqlQuerySumHappyPath(t, newDriver, wantFrames)
		})
		t.Run("partial data", func(t *testing.T) {
			runSqlQuerySumPartialResults(t, newDriver, wantFrames)
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

func TestExtractColumnToField(t *testing.T) {
	testCases := []struct {
		column string
		want   *data.Field
	}{
		{column: "__double", want: data.NewField("__double", nil, []float64{0, 0.1111111111111111, 0.2222222222222222})},
		{column: "__float", want: data.NewField("__float", nil, []float32{0, 0.11111111, 0.22222222})},
		{column: "__int", want: data.NewField("__int", nil, []int32{0, 111111, 222222})},
		{column: "__long", want: data.NewField("__long", nil, []int64{0, 111111111111111, 222222222222222})},
		{column: "__string", want: data.NewField("__string", nil, []string{"row_0", "row_1", "row_2"})},
		{column: "__bytes", want: data.NewField("__bytes", nil, []string{"726f775f30", "726f775f31", "726f775f32"})},
		{column: "__bool", want: data.NewField("__bool", nil, []bool{true, false, true})},
		{column: "__big_decimal", want: data.NewField("__big_decimal", nil,
			[]string{"100000000000000000000", "100000000000000000001", "100000000000000000002"})},
		{column: "__json", want: data.NewField("__json", nil, []json.RawMessage{
			json.RawMessage(`{"key1":"val1","key2":2,"key3":["val3_1","val3_2"]}`),
			json.RawMessage(`{"key1":"val1","key2":2,"key3":["val3_1","val3_2"]}`),
			json.RawMessage(`{"key1":"val1","key2":2,"key3":["val3_1","val3_2"]}`)})},
		{column: "__timestamp", want: data.NewField("__timestamp", nil, []time.Time{
			time.Date(2024, time.November, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, time.November, 1, 0, 0, 1, 0, time.UTC),
			time.Date(2024, time.November, 1, 0, 0, 2, 0, time.UTC)})},
		{column: "__map_string_long", want: data.NewField("__map_string_long", nil, []json.RawMessage{
			json.RawMessage(`{"key1":1,"key2":2}`),
			json.RawMessage(`{"key1":1,"key2":2}`),
			json.RawMessage(`{"key1":1,"key2":2}`)})},
		{column: "__map_string_string", want: data.NewField("__map_string_string", nil, []json.RawMessage{
			json.RawMessage(`{"key1":"val1","key2":"val2"}`),
			json.RawMessage(`{"key1":"val1","key2":"val2"}`),
			json.RawMessage(`{"key1":"val1","key2":"val2"}`)})},
	}

	client := test_helpers.SetupPinotAndCreateClient(t)
	resp, err := client.ExecuteSqlQuery(context.Background(),
		pinot.NewSqlQuery(`select * from "allDataTypes" order by "__timestamp" asc limit 3`))
	require.NoError(t, err, "client.ExecuteSqlQuery()")
	require.True(t, resp.HasData(), "resp.HasData()")

	for _, tt := range testCases {
		t.Run(tt.column, func(t *testing.T) {
			colIdx, err := pinot.GetColumnIdx(resp.ResultTable, tt.column)
			require.NoError(t, err)
			got, err := ExtractColumnAsField(resp.ResultTable, colIdx)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
