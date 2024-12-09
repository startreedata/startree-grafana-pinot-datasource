package dataquery

import (
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"time"
)

type QueryType string

const (
	QueryTypePinotQl            QueryType = "PinotQL"
	QueryTypePromQl             QueryType = "PromQL"
	QueryTypePinotVariableQuery QueryType = "PinotVariableQuery"
)

type EditorMode string

const (
	EditorModeBuilder EditorMode = "Builder"
	EditorModeCode    EditorMode = "Code"
)

type PinotDataQuery struct {
	Hide         bool          `json:"hide"`
	QueryType    QueryType     `json:"queryType"`
	EditorMode   EditorMode    `json:"editorMode"`
	TableName    string        `json:"tableName"`
	DisplayType  string        `json:"displayType"`
	IntervalSize time.Duration `json:"intervalSize"`

	// Sql builder query
	TimeColumn          string            `json:"timeColumn"`
	MetricColumn        string            `json:"metricColumn"`
	GroupByColumns      []string          `json:"groupByColumns"`
	AggregationFunction string            `json:"aggregationFunction"`
	Limit               int64             `json:"limit"`
	DimensionFilters    []DimensionFilter `json:"filters"`
	Granularity         string            `json:"granularity"`
	OrderByClauses      []OrderByClause   `json:"orderBy"`
	QueryOptions        []QueryOption     `json:"queryOptions"`
	Legend              string            `json:"legend"`
	GroupByColumnsV2    []ComplexField    `json:"groupByColumnsV2"`

	// Sql code query
	PinotQlCode       string `json:"pinotQlCode"`
	TimeColumnAlias   string `json:"timeColumnAlias"`
	TimeColumnFormat  string `json:"timeColumnFormat"`
	MetricColumnAlias string `json:"metricColumnAlias"`
	LogColumnAlias    string `json:"logColumnAlias"`

	// Variable query
	VariableQuery struct {
		VariableType string `json:"variableType"`
		PinotQlCode  string `json:"pinotQlCode"`
		ColumnName   string `json:"columnName"`
		ColumnType   string `json:"columnType"`
	} `json:"variableQuery"`

	// PromQl code
	PromQlCode string `json:"promQlCode"`
}

type TimeRange struct {
	To   time.Time `json:"to"`
	From time.Time `json:"from"`
}

type DimensionFilter struct {
	ColumnName string   `json:"columnName"`
	ColumnKey  string   `json:"columnKey,omitempty"`
	ValueExprs []string `json:"valueExprs"`
	Operator   string   `json:"operator"`
}

type ComplexField struct {
	Name string `json:"name"`
	Key  string `json:"key,omitempty"`
}

type OrderByClause struct {
	ColumnName string `json:"columnName"`
	ColumnKey  string `json:"columnKey,omitempty"`
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
