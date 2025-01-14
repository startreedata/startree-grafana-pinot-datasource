package dataquery

import (
	"context"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"testing"
	"time"
)

func init() {
	log.Disable()
}

func BenchmarkPinotQlBuilderDriver_Execute(b *testing.B) {
	client := pinotlib.NewPinotClient(pinotlib.PinotClientProperties{
		ControllerUrl: pinottest.ControllerUrl,
		BrokerUrl:     pinottest.BrokerUrl,
	})
	params := TimeSeriesBuilderParams{
		TimeRange: TimeRange{
			From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 10, 1, 5, 0, 0, 0, time.UTC),
		},
		IntervalSize:        1 * time.Minute,
		TableName:           "benchmark",
		TimeColumn:          "ts",
		MetricColumn:        ComplexField{Name: "value"},
		GroupByColumns:      []ComplexField{{Name: "pattern"}, {Name: "fabric"}},
		AggregationFunction: "SUM",
		Limit:               1_000_000_000,
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		result := ExecuteTimeSeriesBuilderQuery(context.Background(), client, params)
		if result.Error != nil {
			b.Fatal(result.Error)
		}
	}
}
