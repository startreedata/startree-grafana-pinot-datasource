package dataquery

import (
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"time"
)

type QueryType string

func (x QueryType) String() string { return string(x) }

const (
	QueryTypePinotQl            QueryType = "PinotQL"
	QueryTypePromQl             QueryType = "PromQL"
	QueryTypePinotVariableQuery QueryType = "PinotVariableQuery"
)

type EditorMode string

func (x EditorMode) String() string { return string(x) }

const (
	EditorModeBuilder EditorMode = "Builder"
	EditorModeCode    EditorMode = "Code"
)

type DisplayType string

func (x DisplayType) String() string { return string(x) }

const (
	DisplayTypeTable       DisplayType = "TABLE"
	DisplayTypeTimeSeries  DisplayType = "TIMESERIES"
	DisplayTypeLogs        DisplayType = "LOGS"
	DisplayTypeAnnotations DisplayType = "ANNOTATIONS"
)

type VariableQueryType string

const (
	VariableQueryTypeTableList      VariableQueryType = "TABLE_LIST"
	VariableQueryTypeColumnList     VariableQueryType = "COLUMN_LIST"
	VariableQueryTypeDistinctValues VariableQueryType = "DISTINCT_VALUES"
	VariableQueryTypePinotQlCode    VariableQueryType = "PINOT_QL_CODE"
)

type ColumnType string

const (
	ColumnTypeDateTime  ColumnType = "DATETIME"
	ColumnTypeMetric    ColumnType = "METRIC"
	ColumnTypeDimension ColumnType = "DIMENSION"
	ColumnTypeAll       ColumnType = "ALL"
)

const (
	AggregationFunctionCount = "COUNT"
	AggregationFunctionNone  = "NONE"
)

type DataQuery struct {
	TimeRange     TimeRange     `json:"-"`
	MaxDataPoints int64         `json:"-"`
	IntervalSize  time.Duration `json:"-"`

	Hide        bool        `json:"hide"`
	QueryType   QueryType   `json:"queryType"`
	EditorMode  EditorMode  `json:"editorMode"`
	DisplayType DisplayType `json:"displayType"`

	TableName    string        `json:"tableName"`
	QueryOptions []QueryOption `json:"queryOptions"`
	SeriesLimit  int           `json:"seriesLimit"`

	// Sql builder query
	TimeColumn          string            `json:"timeColumn"`
	MetricColumn        string            `json:"metricColumn"`
	GroupByColumns      []string          `json:"groupByColumns"`
	AggregationFunction string            `json:"aggregationFunction"`
	Limit               int64             `json:"limit"`
	DimensionFilters    []DimensionFilter `json:"filters"`
	Granularity         string            `json:"granularity"`
	OrderByClauses      []OrderByClause   `json:"orderBy"`
	Legend              string            `json:"legend"`
	MetricColumnV2      ComplexField      `json:"metricColumnV2"`
	GroupByColumnsV2    []ComplexField    `json:"groupByColumnsV2"`
	MetadataColumns     []ComplexField    `json:"metadataColumns"`
	LogColumn           ComplexField      `json:"logColumn"`
	JsonExtractors      []JsonExtractor   `json:"jsonExtractors"`
	RegexpExtractors    []RegexpExtractor `json:"regexpExtractors"`

	// Sql code query
	PinotQlCode       string `json:"pinotQlCode"`
	TimeColumnAlias   string `json:"timeColumnAlias"`
	TimeColumnFormat  string `json:"timeColumnFormat"`
	MetricColumnAlias string `json:"metricColumnAlias"`
	LogColumnAlias    string `json:"logColumnAlias"`

	// Variable query
	VariableQuery struct {
		VariableType VariableQueryType `json:"variableType"`
		PinotQlCode  string            `json:"pinotQlCode"`
		ColumnName   string            `json:"columnName"`
		ColumnType   ColumnType        `json:"columnType"`
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

type RegexpExtractor struct {
	Source  ComplexField `json:"source"`
	Pattern string       `json:"pattern"`
	Group   int          `json:"group"`
	Alias   string       `json:"alias"`
}

type JsonExtractor struct {
	Source     ComplexField `json:"source"`
	Path       string       `json:"path"`
	ResultType string       `json:"resultType"`
	Alias      string       `json:"alias"`
}

func (query *DataQuery) ReadFrom(backendQuery backend.DataQuery) error {
	if err := json.Unmarshal(backendQuery.JSON, &query); err != nil {
		return fmt.Errorf("failed to unmarshal query model: %w", err)
	}
	query.TimeRange = TimeRange{To: backendQuery.TimeRange.To, From: backendQuery.TimeRange.From}
	query.IntervalSize = backendQuery.Interval
	query.MaxDataPoints = backendQuery.MaxDataPoints

	return nil
}
