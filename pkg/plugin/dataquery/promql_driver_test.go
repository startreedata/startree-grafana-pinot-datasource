package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPromQlDriver_Execute(t *testing.T) {
	client := test_helpers.SetupPinotAndCreateClient(t)

	t.Run("happy path", func(t *testing.T) {
		driver := NewPromQlCodeDriver(PromQlCodeDriverParams{
			PinotClient: client,
			TableName:   "infraMetrics",
			PromQlCode:  `http_request_handled{path="/app",method="GET",status=~"200|400"}`,
			TimeRange: TimeRange{
				From: time.Unix(1726617600, 0).UTC(),
				To:   time.Unix(1726617900, 0).UTC(),
			},
			IntervalSize: 60 * time.Second,
			Legend:       "legend",
		})

		got := driver.Execute(context.Background())
		assert.Equal(t, backend.StatusOK, got.Status, "DataResponse.Status")
		assert.Equal(t, data.Frames{
			data.NewFrame("",
				data.NewField("time", nil, []time.Time{
					time.Unix(1726617600, 0).UTC(),
					time.Unix(1726617660, 0).UTC(),
					time.Unix(1726617720, 0).UTC(),
				}).SetConfig(&data.FieldConfig{
					Interval: float64(60 * time.Second.Milliseconds()),
				}),
				data.NewField("", data.Labels{
					"__name__": "http_request_handled",
					"method":   "GET",
					"path":     "/app",
					"status":   "200",
				}, []float64{
					24022, 48066, 60102,
				}).SetConfig(&data.FieldConfig{
					DisplayNameFromDS: "legend",
				}),
			),

			data.NewFrame("",
				data.NewField("time", nil, []time.Time{
					time.Unix(1726617600, 0).UTC(),
					time.Unix(1726617660, 0).UTC(),
					time.Unix(1726617720, 0).UTC(),
				}).SetConfig(&data.FieldConfig{
					Interval: float64(60 * time.Second.Milliseconds()),
				}),
				data.NewField("", data.Labels{
					"__name__": "http_request_handled",
					"method":   "GET",
					"path":     "/app",
					"status":   "400",
				}, []float64{
					4018, 8045, 10061,
				}).SetConfig(&data.FieldConfig{
					DisplayNameFromDS: "legend",
				}),
			),
		}, got.Frames, "DataResponse.Frames")
		assert.Empty(t, got.ErrorSource, "DataResponse.ErrorSource")
		assert.NoError(t, got.Error, "DataResponse.Error")
	})

	t.Run("empty query", func(t *testing.T) {
		driver := NewPromQlCodeDriver(PromQlCodeDriverParams{
			PinotClient: client,
			TableName:   "infraMetrics",
			PromQlCode:  ``,
			TimeRange: TimeRange{
				From: time.Unix(1726617600, 0).UTC(),
				To:   time.Unix(1726617900, 0).UTC(),
			},
			IntervalSize: 60 * time.Second,
			Legend:       "legend",
		})

		got := driver.Execute(context.Background())
		assert.Equal(t, backend.StatusOK, got.Status, "DataResponse.Status")
		assert.Empty(t, got.Frames, "DataResponse.Frames")
		assert.Empty(t, got.ErrorSource, "DataResponse.ErrorSource")
		assert.NoError(t, got.Error, "DataResponse.Error")
	})

	t.Run("query error", func(t *testing.T) {
		driver := NewPromQlCodeDriver(PromQlCodeDriverParams{
			PinotClient: client,
			TableName:   "infraMetrics",
			PromQlCode:  `Not A Prometheus Query`,
			TimeRange: TimeRange{
				From: time.Unix(1726617600, 0).UTC(),
				To:   time.Unix(1726617900, 0).UTC(),
			},
			IntervalSize: 60 * time.Second,
			Legend:       "legend",
		})

		got := driver.Execute(context.Background())
		assert.Equal(t, backend.StatusInternal, got.Status, "DataResponse.Status")
		assert.Empty(t, got.Frames, "DataResponse.Frames")
		assert.Equal(t, backend.ErrorSourcePlugin, got.ErrorSource, "DataResponse.ErrorSource")
		assert.ErrorContains(t, got.Error, "Invalid query", "DataResponse.Error")
	})
}
