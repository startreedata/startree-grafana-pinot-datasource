package pinotlib

import (
	"context"
	"encoding/json"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
	"time"
)

func TestPinotClient_ListTimeSeriesTables(t *testing.T) {
	client := setupPinotAndCreateClient(t)

	got, err := client.ListTimeSeriesTables(context.Background())
	require.NoError(t, err)
	assert.Subset(t, got, []string{pinottest.InfraMetricsTableName})
}

func TestPinotClient_ListTimeSeriesMetrics(t *testing.T) {
	client := setupPinotAndCreateClient(t)

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := client.ListTimeSeriesMetrics(ctx, TimeSeriesMetricNamesQuery{
			TableName: "infraMetrics",
			From:      time.Unix(1726617600, 0),
			To:        time.Unix(1726617735, 0),
		})
		assert.Contains(t, err.Error(), context.Canceled.Error())
	})

	t.Run("tableName=infraMetrics", func(t *testing.T) {
		got, err := client.ListTimeSeriesMetrics(context.Background(), TimeSeriesMetricNamesQuery{
			TableName: "infraMetrics",
			From:      time.Unix(1726617600, 0),
			To:        time.Unix(1726617735, 0),
		})
		require.NoError(t, err)
		sort.Strings(got)
		assert.Equal(t, []string{"db_record_write", "http_request_handled"}, got)
	})

	t.Run("tableName=timeSeriesWithMapLabels", func(t *testing.T) {
		got, err := client.ListTimeSeriesMetrics(context.Background(), TimeSeriesMetricNamesQuery{
			TableName: "infraMetrics",
			From:      time.Unix(1726617600, 0),
			To:        time.Unix(1726617735, 0),
		})
		require.NoError(t, err)
		sort.Strings(got)
		assert.Equal(t, []string{"db_record_write", "http_request_handled"}, got)
	})
}

func TestPinotClient_ListTimeSeriesLabelNames(t *testing.T) {
	client := setupPinotAndCreateClient(t)

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := client.ListTimeSeriesLabelNames(ctx, TimeSeriesLabelNamesQuery{
			TableName:  "infraMetrics",
			MetricName: "http_request_handled",
			From:       time.Unix(1726617600, 0),
			To:         time.Unix(1726617735, 0),
		})
		assert.Contains(t, err.Error(), context.Canceled.Error())
	})

	t.Run("tableName=infraMetrics", func(t *testing.T) {
		got, err := client.ListTimeSeriesLabelNames(context.Background(), TimeSeriesLabelNamesQuery{
			TableName:  "infraMetrics",
			MetricName: "http_request_handled",
			From:       time.Unix(1726617600, 0),
			To:         time.Unix(1726617735, 0),
		})

		require.NoError(t, err)
		sort.Strings(got)
		assert.Equal(t, []string{"method", "path", "status"}, got)
	})

	t.Run("tableName=timeSeriesWithMapLabels", func(t *testing.T) {
		got, err := client.ListTimeSeriesLabelNames(context.Background(), TimeSeriesLabelNamesQuery{
			TableName:  "timeSeriesWithMapLabels",
			MetricName: "http_request_handled",
			From:       time.Unix(1726617600, 0),
			To:         time.Unix(1726617735, 0),
		})

		require.NoError(t, err)
		sort.Strings(got)
		assert.Equal(t, []string{"method", "path", "status"}, got)
	})
}

func TestPinotClient_ListTimeSeriesLabelValues(t *testing.T) {
	client := setupPinotAndCreateClient(t)

	t.Run("context cancelled", func(t *testing.T) {
		_, err := client.ListTimeSeriesLabelValues(cancelledCtx(), TimeSeriesLabelValuesQuery{
			TableName:  "infraMetrics",
			MetricName: "http_request_handled",
			LabelName:  "path",
			From:       time.Unix(1726617600, 0),
			To:         time.Unix(1726617735, 0),
		})
		assert.Contains(t, err.Error(), context.Canceled.Error())
	})

	t.Run("tableName=infraMetrics", func(t *testing.T) {
		got, err := client.ListTimeSeriesLabelValues(context.Background(), TimeSeriesLabelValuesQuery{
			TableName:  "infraMetrics",
			MetricName: "http_request_handled",
			LabelName:  "path",
			From:       time.Unix(1726617600, 0),
			To:         time.Unix(1726617735, 0),
		})
		require.NoError(t, err)
		sort.Strings(got)
		assert.Equal(t, []string{"/api", "/app"}, got)
	})

	t.Run("tableName=timeSeriesWithMapLabels", func(t *testing.T) {
		got, err := client.ListTimeSeriesLabelValues(context.Background(), TimeSeriesLabelValuesQuery{
			TableName:  "timeSeriesWithMapLabels",
			MetricName: "http_request_handled",
			LabelName:  "path",
			From:       time.Unix(1726617600, 0),
			To:         time.Unix(1726617735, 0),
		})
		require.NoError(t, err)
		sort.Strings(got)
		assert.Equal(t, []string{"/api", "/app"}, got)
	})
}

func TestPinotClient_IsTimeSeriesTable(t *testing.T) {
	client := setupPinotAndCreateClient(t)

	t.Run("context cancelled", func(t *testing.T) {
		_, err := client.IsTimeSeriesTable(cancelledCtx(), "infraMetrics")
		assert.Contains(t, err.Error(), context.Canceled.Error())
	})

	t.Run("table=infraMetrics", func(t *testing.T) {
		got, err := client.IsTimeSeriesTable(context.Background(), "infraMetrics")
		assert.NoError(t, err)
		assert.True(t, got)
	})

	t.Run("table=timeSeriesWithMapLabels", func(t *testing.T) {
		got, err := client.IsTimeSeriesTable(context.Background(), "timeSeriesWithMapLabels")
		assert.NoError(t, err)
		assert.True(t, got)
	})

	t.Run("table=benchmark", func(t *testing.T) {
		got, err := client.IsTimeSeriesTable(context.Background(), "benchmark")
		assert.NoError(t, err)
		assert.False(t, got)
	})
}

func TestIsTimeSeriesTableSchema(t *testing.T) {
	t.Run("tsIsTimestamp", func(t *testing.T) {
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

	t.Run("tsIsLong", func(t *testing.T) {
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

	t.Run("unsupported", func(t *testing.T) {
		schema := TableSchema{
			SchemaName: "startree_metrics_analytics",
			DimensionFieldSpecs: []DimensionFieldSpec{
				{Name: "metricZZ", DataType: "STRING"},
				{Name: "labels", DataType: "JSON"},
			},
			MetricFieldSpecs: []MetricFieldSpec{
				{Name: "value", DataType: "DOUBLE"},
			},
			DateTimeFieldSpecs: []DateTimeFieldSpec{
				{Name: "ts", DataType: "LONG"},
			},
		}
		assert.Equal(t, false, IsTimeSeriesTableSchema(schema))
	})
}

func TestTimeSeriesResult_UnmarshalJSON(t *testing.T) {
	payload := `{
        "metric": {
          "__name__": "http_request_handled",
          "metric": "http_request_handled",
          "labels": "{\"method\":\"GET\",\"path\":\"/app\",\"status\":\"200\"}"
        },
        "values": [
          [
            1726617600,
            "24022.0"
          ],
          [
            1726617660,
            "48066.0"
          ],
          [
            1726617720,
            null
          ]
        ]
      }`

	var got TimeSeriesResult
	err := json.Unmarshal([]byte(payload), &got)
	require.NoError(t, err)
	assert.Equal(t, TimeSeriesResult{
		Metric: map[string]string{
			"__name__": "http_request_handled",
			"method":   "GET",
			"path":     "/app",
			"status":   "200",
		},
		Timestamps: []time.Time{
			time.Unix(1726617600, 0).UTC(),
			time.Unix(1726617660, 0).UTC(),
		},
		Values: []float64{
			24022.0,
			48066.0,
		},
	}, got)
}

func TestPinotClient_ExecuteTimeSeriesQuery(t *testing.T) {
	client := setupPinotAndCreateClient(t)

	resp, err := client.ExecuteTimeSeriesQuery(context.Background(), &TimeSeriesRangeQuery{
		Language:  TimeSeriesQueryLanguagePromQl,
		Query:     `http_request_handled{path="/app",method="GET",status=~"200|400"}`,
		Start:     time.Unix(1726617600, 0).UTC(),
		End:       time.Unix(1726617900, 0).UTC(),
		Step:      60 * time.Second,
		TableName: "infraMetrics_OFFLINE",
	})
	require.NoError(t, err)

	assert.Equal(t, &TimeSeriesQueryResponse{
		Status: "success",
		Data: TimeSeriesData{
			ResultType: "matrix",
			Result: []TimeSeriesResult{
				{
					Metric: map[string]string{
						"__name__": "http_request_handled",
						"method":   "GET",
						"path":     "/app",
						"status":   "200",
					},
					Timestamps: []time.Time{
						time.Unix(1726617600, 0).UTC(),
						time.Unix(1726617660, 0).UTC(),
						time.Unix(1726617720, 0).UTC(),
					},
					Values: []float64{24022, 48066, 60102},
				},
				{
					Metric: map[string]string{
						"__name__": "http_request_handled",
						"method":   "GET",
						"path":     "/app",
						"status":   "400",
					},
					Timestamps: []time.Time{
						time.Unix(1726617600, 0).UTC(),
						time.Unix(1726617660, 0).UTC(),
						time.Unix(1726617720, 0).UTC(),
					},
					Values: []float64{4018, 8045, 10061},
				},
			},
		},
	}, resp)
}
