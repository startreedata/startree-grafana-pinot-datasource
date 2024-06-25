package plugin

import (
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"time"
)

type PinotDataQuery struct {
	QueryType           string        `json:"editorType"`
	DatabaseName        string        `json:"databaseName"`
	TableName           string        `json:"tableName"`
	RawSql              string        `json:"rawSql"`
	TimeColumn          string        `json:"timeColumn"`
	MetricColumn        string        `json:"metricColumn"`
	DimensionColumns    []string      `json:"dimensionColumns"`
	AggregationFunction string        `json:"aggregationFunction"`
	IntervalSize        time.Duration `json:"intervalSize"`
	TimeRange           TimeRange     `json:"timeRange"`
}

type TimeRange struct {
	To   time.Time `json:"to"`
	From time.Time `json:"from"`
}

func PinotDataQueryFrom(query backend.DataQuery) (PinotDataQuery, error) {
	var pinotDataQuery PinotDataQuery
	if err := json.Unmarshal(query.JSON, &pinotDataQuery); err != nil {
		return PinotDataQuery{}, fmt.Errorf("failed to unmarshal query model: %w", err)
	}
	if pinotDataQuery.IntervalSize == 0 {
		pinotDataQuery.IntervalSize = query.Interval
	}
	return pinotDataQuery, nil
}
