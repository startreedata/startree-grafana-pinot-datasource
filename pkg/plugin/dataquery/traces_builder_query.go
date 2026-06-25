package dataquery

import (
	"context"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
)

var _ ExecutableQuery = TracesBuilderQuery{}

// TracesBuilderQuery renders an OpenTelemetry span query and shapes the result into Grafana's trace
// DataFrame. The span start time is the table's TimeColumn (reused for both the projected startTime
// and the time filter). When TraceId is set the query runs in "find trace by ID" mode (all spans of
// one trace, chronological); otherwise it runs in "search" mode (matching spans, most recent first).
type TracesBuilderQuery struct {
	TimeRange          TimeRange
	TableName          string
	TimeColumn         string
	TraceIdColumn      ComplexField
	SpanIdColumn       ComplexField
	ParentSpanIdColumn ComplexField
	ServiceNameColumn  ComplexField
	SpanNameColumn     ComplexField
	DurationColumn     ComplexField
	DurationUnit       string
	TagsColumn         ComplexField
	StatusColumn       ComplexField
	TraceId            string
	DimensionFilters   []DimensionFilter
	QueryOptions       []QueryOption
	Limit              int64

	// Datasource context for the trace-to-logs data link.
	DatasourceUID  string
	DatasourceName string
	TracesToLogs   TracesToLogsConfig
}

func (query TracesBuilderQuery) Validate() error {
	switch {
	case query.TableName == "":
		return errors.New("table name is required")
	case query.TimeColumn == "":
		return errors.New("time column is required")
	case query.TraceIdColumn.Name == "":
		return errors.New("trace ID column is required")
	case query.SpanIdColumn.Name == "":
		return errors.New("span ID column is required")
	case query.DurationColumn.Name == "":
		return errors.New("duration column is required")
	default:
		return nil
	}
}

func (query TracesBuilderQuery) Execute(client *pinot.Client, ctx context.Context) backend.DataResponse {
	if err := query.Validate(); err != nil {
		return NewBadRequestErrorResponse(err)
	}

	sqlQuery, timeColumnFormat, err := query.RenderSqlQuery(ctx, client)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, client, sqlQuery)
	if !ok {
		return backendResp
	}

	frame, err := BuildTracesDataFrame(results, timeColumnFormat, query.durationFactor(), query.traceToLogsLink())
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	return NewSqlQueryDataResponse(frame, exceptions)
}

func (query TracesBuilderQuery) RenderSqlQuery(ctx context.Context, client *pinot.Client) (pinot.SqlQuery, pinot.DateTimeFormat, error) {
	tableSchema, err := client.GetTableSchema(ctx, query.TableName)
	if err != nil {
		return pinot.SqlQuery{}, pinot.DateTimeFormat{}, err
	}

	timeColumnFormat, err := pinot.GetTimeColumnFormat(tableSchema, query.TimeColumn)
	if err != nil {
		return pinot.SqlQuery{}, pinot.DateTimeFormat{}, err
	}

	// Expand Grafana plugin macros inside any filter's SubqueryExpr — same rationale as the other
	// builders (a Pinot Query variable's backing SQL may contain $__table()/$__timeFilter()/etc).
	expandedFilters, err := ExpandSubqueryMacros(ctx, query.DimensionFilters, MacroEngine{
		TableName:   query.TableName,
		TableSchema: tableSchema,
		TimeRange:   query.TimeRange,
		TimeAlias:   BuilderTimeColumn,
	})
	if err != nil {
		return pinot.SqlQuery{}, pinot.DateTimeFormat{}, err
	}

	sql, err := pinot.RenderTraceSql(pinot.TraceSqlParams{
		TableNameExpr:        pinot.ObjectExpr(query.TableName),
		SelectExprs:          query.selectExprs(),
		DimensionFilterExprs: FilterExprsFrom(expandedFilters),
		TraceIdFilterExpr:    query.traceIdFilterExpr(),
		OrderByExpr:          query.orderByExpr(),
		Limit:                query.resolveLimit(),
		TimeFilterExpr: pinot.TimeFilterExpr(pinot.TimeFilter{
			Column: query.TimeColumn,
			Format: timeColumnFormat,
			From:   query.TimeRange.From,
			To:     query.TimeRange.To,
		}),
	})
	if err != nil {
		return pinot.SqlQuery{}, pinot.DateTimeFormat{}, err
	}

	return newSqlQueryWithOptions(sql, query.QueryOptions), timeColumnFormat, nil
}

func (query TracesBuilderQuery) RenderSqlWithMacros() (string, error) {
	sql, err := pinot.RenderTraceSql(pinot.TraceSqlParams{
		TableNameExpr:        MacroExprFor(MacroTable),
		SelectExprs:          query.selectExprs(),
		DimensionFilterExprs: FilterExprsFrom(query.DimensionFilters),
		TraceIdFilterExpr:    query.traceIdFilterExpr(),
		OrderByExpr:          query.orderByExpr(),
		Limit:                query.resolveLimit(),
		TimeFilterExpr:       MacroExprFor(MacroTimeFilter, pinot.ObjectExpr(query.TimeColumn).String()),
	})
	if err != nil {
		return "", err
	}
	return newSqlQueryWithOptions(sql, query.QueryOptions).RenderSql(), nil
}

// selectExprs projects each mapped span column under the fixed alias BuildTracesDataFrame reads.
// Required columns come first; optional columns are included only when the user mapped them.
func (query TracesBuilderQuery) selectExprs() []pinot.SqlExpr {
	exprs := []pinot.SqlExpr{
		aliasedTraceExpr(complexFieldExpr(query.TraceIdColumn), TraceFieldTraceID),
		aliasedTraceExpr(complexFieldExpr(query.SpanIdColumn), TraceFieldSpanID),
		aliasedTraceExpr(pinot.ObjectExpr(query.TimeColumn), TraceFieldStartTime),
		aliasedTraceExpr(complexFieldExpr(query.DurationColumn), TraceFieldDuration),
	}
	for _, optional := range []struct {
		column ComplexField
		alias  string
	}{
		{query.ParentSpanIdColumn, TraceFieldParentSpanID},
		{query.ServiceNameColumn, TraceFieldServiceName},
		{query.SpanNameColumn, TraceFieldOperationName},
		{query.StatusColumn, TraceFieldStatusCode},
		{query.TagsColumn, TraceFieldTags},
	} {
		if optional.column.Name != "" {
			exprs = append(exprs, aliasedTraceExpr(complexFieldExpr(optional.column), optional.alias))
		}
	}
	return exprs
}

// traceIdFilterExpr returns the `"col" = 'id'` filter for find-by-ID mode, or "" in search mode.
func (query TracesBuilderQuery) traceIdFilterExpr() pinot.SqlExpr {
	if query.TraceId == "" {
		return ""
	}
	return pinot.ColumnFilterExpr(pinot.ColumnFilter{
		ColumnName: query.TraceIdColumn.Name,
		ColumnKey:  query.TraceIdColumn.Key,
		Operator:   pinot.FilterOpEquals,
		ValueExprs: []string{pinot.StringLiteralExpr(query.TraceId).String()},
	})
}

// orderByExpr sorts spans chronologically within a single trace (find-by-ID) and most-recent-first
// for search results.
func (query TracesBuilderQuery) orderByExpr() pinot.SqlExpr {
	direction := "DESC"
	if query.TraceId != "" {
		direction = "ASC"
	}
	return pinot.OrderByExpr(pinot.ObjectExpr(query.TimeColumn), direction)
}

// durationFactor converts the raw duration column into milliseconds (what Grafana's trace view
// expects). Defaults to milliseconds when the unit is unset or unrecognized.
func (query TracesBuilderQuery) durationFactor() float64 {
	switch query.DurationUnit {
	case "ns":
		return 1e-6
	case "us", "µs":
		return 1e-3
	case "s":
		return 1e3
	default: // "ms" or unset
		return 1
	}
}

func (query TracesBuilderQuery) traceToLogsLink() *TraceToLogsLink {
	if !query.TracesToLogs.IsConfigured() {
		return nil
	}
	return &TraceToLogsLink{
		DatasourceUID:  query.DatasourceUID,
		DatasourceName: query.DatasourceName,
		LogsTable:      query.TracesToLogs.Table,
		TraceIdColumn:  query.TracesToLogs.TraceIdColumn,
		TimeColumn:     query.TracesToLogs.TimeColumn,
		LogColumn:      query.TracesToLogs.LogColumn,
	}
}

func (query TracesBuilderQuery) resolveLimit() int64 {
	if query.Limit >= 1 {
		return query.Limit
	}
	return DefaultQueryLimit
}

func complexFieldExpr(field ComplexField) pinot.SqlExpr {
	return pinot.ComplexFieldExpr(field.Name, field.Key)
}

func aliasedTraceExpr(expr pinot.SqlExpr, alias string) pinot.SqlExpr {
	return pinot.SqlExpr(fmt.Sprintf("%s AS '%s'", expr, alias))
}
