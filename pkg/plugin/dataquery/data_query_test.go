package dataquery

import (
	"encoding/json"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPinotDataQueryFrom(t *testing.T) {
	want := DataQuery{
		TimeRange:           TimeRange{From: time.Unix(0, 0), To: time.Unix(100, 0)},
		QueryType:           "PinotQL",
		EditorMode:          "Builder",
		TableName:           "m_test_1",
		IntervalSize:        10 * time.Second,
		TimeColumn:          "timestampRoundedMinutes",
		MetricColumn:        "value",
		GroupByColumns:      []string{"type"},
		AggregationFunction: "MAX",
		Limit:               1000000,
		DimensionFilters: []DimensionFilter{
			{
				ColumnName: "type",
				Operator:   "=",
				ValueExprs: []string{"'gauge'"},
			},
		},
		Granularity: "MINUTES",
	}

	var got DataQuery
	err := got.ReadFrom(backend.DataQuery{
		RefID:     "1",
		Interval:  10 * time.Second,
		TimeRange: backend.TimeRange{From: time.Unix(0, 0), To: time.Unix(100, 0)},
		JSON: json.RawMessage(`{
		  "aggregationFunction": "MAX",
		  "databaseName": "default",
		  "datasource": {
			"type": "startree-pinot-datasource",
			"uid": "ddrctq0hu20aoc"
		  },
		  "editorMode": "Builder",
		  "filters": [
			{
			  "columnName": "type",
			  "operator": "=",
			  "valueExprs": [
				"'gauge'"
			  ]
			}
		  ],
		  "granularity": "MINUTES",
		  "groupByColumns": [
			"type"
		  ],
		  "limit": 1000000,
		  "metricColumn": "value",
		  "queryType": "PinotQL",
		  "refId": "A",
		  "tableName": "m_test_1",
		  "timeColumn": "timestampRoundedMinutes",
		  "datasourceId": 1,
		  "intervalMs": 15000,
		  "maxDataPoints": 1296
		}`),
	})

	assert.NoError(t, err)
	assert.Equal(t, want, got)
}
