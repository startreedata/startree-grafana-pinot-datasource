package dataquery

import (
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"testing"
	"time"
)

func TestPinotQlCodeDriver_Execute(t *testing.T) {
	t.Run("display="+DisplayTypeTimeSeries, func(t *testing.T) {
		newDriver := func(testCase DriverTestCase) (Driver, error) {
			params := PinotQlCodeDriverParams{
				PinotClient:       testCase.Client,
				TableName:         testCase.TableName,
				TimeRange:         testCase.TimeRange,
				IntervalSize:      testCase.IntervalSize,
				TableSchema:       testCase.TableSchema,
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
			return NewPinotQlCodeDriver(params)
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

	t.Run("display="+DisplayTypeTable, func(t *testing.T) {
		newDriver := func(testCase DriverTestCase) (Driver, error) {
			params := PinotQlCodeDriverParams{
				PinotClient:       testCase.Client,
				TableName:         testCase.TableName,
				TimeRange:         testCase.TimeRange,
				IntervalSize:      testCase.IntervalSize,
				TableSchema:       testCase.TableSchema,
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
			return NewPinotQlCodeDriver(params)
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

	t.Run("display="+DisplayTypeLogs, func(t *testing.T) {
		newDriver := func(testCase DriverTestCase) (Driver, error) {
			params := PinotQlCodeDriverParams{
				PinotClient:     testCase.Client,
				TableName:       testCase.TableName,
				TimeRange:       testCase.TimeRange,
				IntervalSize:    testCase.IntervalSize,
				TableSchema:     testCase.TableSchema,
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
			return NewPinotQlCodeDriver(params)
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
