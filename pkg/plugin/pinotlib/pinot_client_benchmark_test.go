package pinotlib

import (
	"context"
	"github.com/startreedata/pinot-client-go/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"net/http"
	"testing"
)

// This benchmark compares the Pinot SDK ExecuteSql() method to the simplified query handler used by this plugin.

func BenchmarkPinotQuery(b *testing.B) {
	const sql = `SELECT
    "pattern",
    "fabric",
    DATETIMECONVERT("ts", '1:MILLISECONDS:TIMESTAMP', '1:MILLISECONDS:EPOCH', '1:MINUTES') AS "time",
    SUM("value") AS "metric"
FROM
    "benchmark"
WHERE
    "ts" >= 1727740800000 AND "ts" < 1727758800000
GROUP BY
    "pattern",
    "fabric",
    DATETIMECONVERT("ts", '1:MILLISECONDS:TIMESTAMP', '1:MILLISECONDS:EPOCH', '1:MINUTES')
ORDER BY
    "time" DESC
LIMIT 1000000000;`

	const tableName = "benchmark"

	sdkClient, err := pinot.NewWithConfig(&pinot.ClientConfig{
		BrokerList: []string{pinottest.BrokerUrl},
	})
	if err != nil {
		b.Fatal(err)
	}

	pluginClient := NewPinotClient(http.DefaultClient, PinotClientProperties{
		ControllerUrl: pinottest.ControllerUrl,
		BrokerUrl:     pinottest.BrokerUrl,
	})

	b.ResetTimer()
	b.Run("sdk_client", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			res, err := sdkClient.ExecuteSQL(tableName, sql)
			if err != nil {
				b.Fatal(err)
			}
			if len(res.Exceptions) > 0 {
				b.Fatal(res.Exceptions[0])
			}
		}
	})
	b.Run("broker_query", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			resp, err := pluginClient.ExecuteSqlQuery(context.Background(), NewSqlQuery(sql))
			if err != nil {
				b.Fatal(err)
			}
			if len(resp.Exceptions) > 0 {
				b.Fatal(resp.Exceptions[0])
			}
		}
	})
}
