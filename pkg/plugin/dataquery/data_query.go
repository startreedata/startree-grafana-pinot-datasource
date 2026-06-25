package dataquery

import (
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
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
	DisplayTypeTraces      DisplayType = "TRACES"
	DisplayTypeAnnotations DisplayType = "ANNOTATIONS"
)

// TracesToLogsConfig is the datasource-level mapping that turns trace spans into a logs query.
// When fully populated, the traces builder attaches an internal data link from each span to a
// PinotQL logs query against Table, filtered by TraceIdColumn = the span's trace id.
type TracesToLogsConfig struct {
	Table         string `json:"tracesToLogsTable"`
	TraceIdColumn string `json:"tracesToLogsTraceIdColumn"`
	TimeColumn    string `json:"tracesToLogsTimeColumn"`
	LogColumn     string `json:"tracesToLogsLogColumn"`
}

func (c TracesToLogsConfig) IsConfigured() bool {
	return c.Table != "" && c.TraceIdColumn != "" && c.TimeColumn != "" && c.LogColumn != ""
}

// DataSourceMeta carries datasource-level context that individual queries need but which does not
// arrive in the per-query JSON: the datasource identity (for internal data links) and the
// trace-to-logs mapping read from datasource settings.
type DataSourceMeta struct {
	UID          string
	Name         string
	TracesToLogs TracesToLogsConfig
}

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

	// Datasource-level context, injected from settings (not part of the query JSON).
	DatasourceUID  string             `json:"-"`
	DatasourceName string             `json:"-"`
	TracesToLogs   TracesToLogsConfig `json:"-"`

	Hide        bool        `json:"hide"`
	QueryType   QueryType   `json:"queryType"`
	EditorMode  EditorMode  `json:"editorMode"`
	DisplayType DisplayType `json:"displayType"`

	TableName    string              `json:"tableName"`
	QueryOptions []QueryOption       `json:"queryOptions"`
	SeriesLimit  int                 `json:"seriesLimit"`
	AdHocFilters []pinot.AdHocFilter `json:"adHocFilters"`

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
	Aggregations        []Aggregation     `json:"aggregations"`
	MetadataColumns     []ComplexField    `json:"metadataColumns"`
	LogColumn           ComplexField      `json:"logColumn"`
	JsonExtractors      []JsonExtractor   `json:"jsonExtractors"`
	RegexpExtractors    []RegexpExtractor `json:"regexpExtractors"`

	// Traces builder query. The start time / time filter reuse TimeColumn above. TraceId, when set,
	// selects "find trace by ID" mode; otherwise the builder runs in "search" mode.
	TraceIdColumn      ComplexField `json:"traceIdColumn"`
	SpanIdColumn       ComplexField `json:"spanIdColumn"`
	ParentSpanIdColumn ComplexField `json:"parentSpanIdColumn"`
	ServiceNameColumn  ComplexField `json:"serviceNameColumn"`
	SpanNameColumn     ComplexField `json:"spanNameColumn"`
	DurationColumn     ComplexField `json:"durationColumn"`
	DurationUnit       string       `json:"durationUnit"`
	TagsColumn         ComplexField `json:"tagsColumn"`
	StatusColumn       ComplexField `json:"statusColumn"`
	TraceId            string       `json:"traceId"`

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
	ColumnName   string   `json:"columnName"`
	ColumnKey    string   `json:"columnKey,omitempty"`
	ValueExprs   []string `json:"valueExprs"`
	Operator     string   `json:"operator"`
	SubqueryExpr string   `json:"subqueryExpr,omitempty"`
}

type ComplexField struct {
	Name string `json:"name"`
	Key  string `json:"key,omitempty"`
}

type Aggregation struct {
	Function string       `json:"function"`
	Column   ComplexField `json:"column"`
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
