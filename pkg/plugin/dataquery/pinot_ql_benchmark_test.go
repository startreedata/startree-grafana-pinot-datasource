package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/logger"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"testing"
	"time"
)

func init() {
	logger.Logger = log.NewNullLogger()
}

func BenchmarkPinotQlBuilderDriver_Execute(b *testing.B) {
	client, err := pinotlib.NewPinotClient(pinotlib.PinotClientProperties{
		ControllerUrl: pinottest.ControllerUrl,
		BrokerUrl:     pinottest.BrokerUrl,
	})
	if err != nil {
		b.Fatal(err)
	}
	schema, err := client.GetTableSchema(context.Background(), "benchmark")
	if err != nil {
		b.Fatal(err)
	}

	params := PinotQlBuilderParams{
		PinotClient: client,
		TableSchema: schema,
		TimeRange: TimeRange{
			From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 10, 1, 5, 0, 0, 0, time.UTC),
		},
		IntervalSize:        1 * time.Minute,
		TableName:           "benchmark",
		TimeColumn:          "ts",
		MetricColumn:        "value",
		GroupByColumns:      []string{"pattern", "fabric"},
		AggregationFunction: "SUM",
		Limit:               1_000_000_000,
	}

	driver, err := NewPinotQlBuilderDriver(params)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		result := driver.Execute(context.Background())
		if result.Error != nil {
			b.Fatal(result.Error)
		}
	}
}
