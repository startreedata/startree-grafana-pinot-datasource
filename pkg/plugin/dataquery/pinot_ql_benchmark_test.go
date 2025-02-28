package dataquery

import (
	"context"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot/pinottest"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"net/http"
	"testing"
	"time"
)

func init() {
	log.Disable()
}

func BenchmarkPinotQlBuilderDriver_Execute(b *testing.B) {
	client := pinot.NewPinotClient(http.DefaultClient, pinot.ClientProperties{
		ControllerUrl: pinottest.ControllerUrl,
		BrokerUrl:     pinottest.BrokerUrl,
	})
	query := TimeSeriesBuilderQuery{
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
		result := query.Execute(client, context.Background())
		if result.Error != nil {
			b.Fatal(result.Error)
		}
	}
}
