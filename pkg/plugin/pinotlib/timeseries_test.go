package pinotlib

import (
	"context"
	"encoding/json"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"sort"
	"testing"
	"time"
)

func TestPinotClient_ExecuteTimeSeriesQuery(t *testing.T) {
	client := setupPinotAndCreateClient(t)

	resp, err := client.ExecuteTimeSeriesQuery(context.Background(), &TimeSeriesRangeQuery{
		Language:  TimeSeriesQueryLanguagePromQl,
		Query:     "http_request_handled",
		Start:     time.Unix(1726617600, 0),
		End:       time.Unix(1726617735, 0),
		Step:      60 * time.Second,
		TableName: pinottest.InfraMetricsTableName,
	})

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var respData struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&respData))
	assert.Equal(t, "success", respData.Status)
}

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

	t.Run("infraMetrics", func(t *testing.T) {
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

	t.Run("infraMetrics", func(t *testing.T) {
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

}

func TestPinotClient_ListTimeSeriesLabelValues(t *testing.T) {
	client := setupPinotAndCreateClient(t)

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := client.ListTimeSeriesLabelValues(ctx, TimeSeriesLabelValuesQuery{
			TableName:  "infraMetrics",
			MetricName: "http_request_handled",
			LabelName:  "path",
			From:       time.Unix(1726617600, 0),
			To:         time.Unix(1726617735, 0),
		})
		assert.Contains(t, err.Error(), context.Canceled.Error())
	})

	t.Run("", func(t *testing.T) {
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
}

func TestPinotClient_IsTimeSeriesTable(t *testing.T) {
	client := setupPinotAndCreateClient(t)

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := client.IsTimeSeriesTable(ctx, "infraMetrics")
		assert.Contains(t, err.Error(), context.Canceled.Error())
	})

	t.Run("infraMetrics", func(t *testing.T) {
		got, err := client.IsTimeSeriesTable(context.Background(), "infraMetrics")
		assert.NoError(t, err)
		assert.True(t, got)
	})

	t.Run("githubEvents", func(t *testing.T) {
		got, err := client.IsTimeSeriesTable(context.Background(), "githubEvents")
		assert.NoError(t, err)
		assert.False(t, got)
	})
}

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
				{Name: "ts", DataType: "TIMESTAMP"},
			},
		}

		assert.Equal(t, true, IsTimeSeriesTableSchema(schema))
	})

	t.Run("isnt", func(t *testing.T) {
		schema := TableSchema{
			SchemaName:          "startree_metrics_analytics",
			DimensionFieldSpecs: nil,
			MetricFieldSpecs:    nil,
			DateTimeFieldSpecs:  nil,
		}
		assert.Equal(t, false, IsTimeSeriesTableSchema(schema))
	})
}
