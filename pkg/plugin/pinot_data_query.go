package plugin

import (
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"time"
)

type PinotDataQuery struct {
	QueryType    string        `json:"queryType"`
	EditorMode   string        `json:"editorMode"`
	DatabaseName string        `json:"databaseName"`
	TableName    string        `json:"tableName"`
	IntervalSize time.Duration `json:"intervalSize"`

	TimeColumn          string            `json:"timeColumn"`
	MetricColumn        string            `json:"metricColumn"`
	GroupByColumns      []string          `json:"groupByColumns"`
	AggregationFunction string            `json:"aggregationFunction"`
	Limit               int64             `json:"limit"`
	DimensionFilters    []DimensionFilter `json:"filters"`
	Granularity         string            `json:"granularity"`
	OrderByClauses      []OrderByClause   `json:"orderBy"`
	QueryOptions        []QueryOption     `json:"queryOptions"`

	PinotQlCode       string `json:"pinotQlCode"`
	TimeColumnAlias   string `json:"timeColumnAlias"`
	TimeColumnFormat  string `json:"timeColumnFormat"`
	MetricColumnAlias string `json:"metricColumnAlias"`
	DisplayType       string `json:"displayType"`
}

type TimeRange struct {
	To   time.Time `json:"to"`
	From time.Time `json:"from"`
}

type DimensionFilter struct {
	ColumnName string   `json:"columnName"`
	ValueExprs []string `json:"valueExprs"`
	Operator   string   `json:"operator"`
}

type OrderByClause struct {
	ColumnName string `json:"columnName"`
	Direction  string `json:"direction"`
}

type QueryOption struct {
	Name  string `json:"name"`
	Value string `json:"value"`
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
