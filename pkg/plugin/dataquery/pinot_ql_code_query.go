package dataquery

import (
	"context"
	"errors"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"time"
)

var _ ExecutableQuery = PinotQlCodeQuery{}

type PinotQlCodeQuery struct {
	Code              string
	TableName         string
	TimeColumnAlias   string
	MetricColumnAlias string
	LogColumnAlias    string
	TimeRange         TimeRange
	IntervalSize      time.Duration
	DisplayType       DisplayType
	Legend            string
	SeriesLimit       int
	AdHocFilters      []pinot.AdHocFilter
	QueryOptions      []QueryOption
	// UsesTimeRange is set by the frontend from the SQL as the user wrote it (before Grafana replaces
	// $__from/$__to). nil means unknown (e.g. alerting or API callers), which never warns.
	UsesTimeRange *bool
}

// NoTimeRangeWarning is shown on code-mode panels whose SQL never references the dashboard time range.
const NoTimeRangeWarning = "This query does not use the dashboard time range, so changing the time picker will not change the results. " +
	"Add a time filter such as `WHERE $__timeFilter(\"<time column>\")` (or just `WHERE $__timeFilter` to use the table's time column)."

func (query PinotQlCodeQuery) Validate() error {
	switch {
	case query.TableName == "":
		return errors.New("field `TableName` is required")
	case query.IntervalSize == 0:
		return errors.New("field `IntervalSize` is required")
	default:
		return nil
	}
}

func (query PinotQlCodeQuery) Execute(client *pinot.Client, ctx context.Context) backend.DataResponse {
	if err := query.Validate(); err != nil {
		return NewBadRequestErrorResponse(err)
	}

	if query.Code == "" {
		return NewEmptyDataResponse()
	}

	var warnings []string
	sqlQuery, err := query.renderSqlQuery(ctx, client, &warnings)
	if err != nil {
		return NewPluginErrorResponse(err)
	}
	if query.UsesTimeRange != nil && !*query.UsesTimeRange {
		warnings = append(warnings, NoTimeRangeWarning)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, client, sqlQuery)
	if !ok {
		return backendResp
	}

	frame, err := query.ExtractResults(results)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	return WithWarningNotices(NewSqlQueryDataResponse(frame, exceptions), warnings)
}

func (query PinotQlCodeQuery) RenderSqlQuery(ctx context.Context, client *pinot.Client) (pinot.SqlQuery, error) {
	return query.renderSqlQuery(ctx, client, nil)
}

func (query PinotQlCodeQuery) renderSqlQuery(ctx context.Context, client *pinot.Client, warnings *[]string) (pinot.SqlQuery, error) {
	tableSchema, err := client.GetTableSchema(ctx, query.TableName)
	if err != nil {
		return pinot.SqlQuery{}, err
	}

	tableConfigs, err := client.ListTableConfigs(ctx, query.TableName)
	if err != nil {
		return pinot.SqlQuery{}, err
	}

	sql, err := MacroEngine{
		TableName:    query.TableName,
		TableSchema:  tableSchema,
		TableConfigs: tableConfigs,
		AdHocFilters: query.AdHocFilters,
		TimeRange:    query.TimeRange,
		IntervalSize: query.IntervalSize,
		TimeAlias:    query.resolveTimeColumnAlias(),
		MetricAlias:  query.resolveMetricColumnAlias(),
		Warnings:     warnings,
	}.ExpandMacros(ctx, query.Code)
	if err != nil {
		return pinot.SqlQuery{}, err
	}

	return newSqlQueryWithOptions(sql, query.QueryOptions), nil
}

func (query PinotQlCodeQuery) ExtractResults(results *pinot.ResultTable) (*data.Frame, error) {
	switch query.DisplayType {
	case DisplayTypeTable, DisplayTypeAnnotations:
		return ExtractTableDataFrame(results, query.resolveTimeColumnAlias())
	case DisplayTypeLogs:
		return ExtractLogsDataFrame(results, query.resolveTimeColumnAlias(), query.resolveLogColumnAlias())
	default:
		return ExtractTimeSeriesDataFrame(TimeSeriesExtractorParams{
			MetricName:        query.resolveMetricColumnAlias(),
			Legend:            query.Legend,
			TimeColumnAlias:   query.resolveTimeColumnAlias(),
			MetricColumnAlias: query.resolveMetricColumnAlias(),
			TimeColumnFormat:  OutputTimeFormat(),
			SeriesLimit:       query.SeriesLimit,
		}, results)
	}
}

func (query PinotQlCodeQuery) resolveTimeColumnAlias() string {
	return getOrFallback(query.TimeColumnAlias, BuilderTimeColumn)
}

func (query PinotQlCodeQuery) resolveMetricColumnAlias() string {
	return getOrFallback(query.MetricColumnAlias, BuilderMetricColumn)
}

func (query PinotQlCodeQuery) resolveLogColumnAlias() string {
	return getOrFallback(query.LogColumnAlias, BuilderLogColumn)
}

func getOrFallback[T interface{ ~string }](value T, fallback T) T {
	if value != "" {
		return value
	} else {
		return fallback
	}
}
