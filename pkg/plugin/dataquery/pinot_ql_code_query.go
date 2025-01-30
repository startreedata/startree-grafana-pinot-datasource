package dataquery

import (
	"context"
	"errors"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
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
}

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

func (query PinotQlCodeQuery) Execute(ctx context.Context, client *pinotlib.PinotClient) backend.DataResponse {
	if err := query.Validate(); err != nil {
		return NewBadRequestErrorResponse(err)
	}

	if query.Code == "" {
		return NewEmptyDataResponse()
	}

	sqlQuery, err := query.RenderSqlQuery(ctx, client)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, client, sqlQuery)
	if !ok {
		return backendResp
	}

	frame, err := query.ExtractResults(results)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	return NewSqlQueryDataResponse(frame, exceptions)
}

func (query PinotQlCodeQuery) RenderSqlQuery(ctx context.Context, client *pinotlib.PinotClient) (pinotlib.SqlQuery, error) {
	tableSchema, err := client.GetTableSchema(ctx, query.TableName)
	if err != nil {
		return pinotlib.SqlQuery{}, err
	}

	tableConfigs, err := client.ListTableConfigs(ctx, query.TableName)
	if err != nil {
		return pinotlib.SqlQuery{}, err
	}

	sql, err := MacroEngine{
		TableName:    query.TableName,
		TableSchema:  tableSchema,
		TableConfigs: tableConfigs,
		TimeRange:    query.TimeRange,
		IntervalSize: query.IntervalSize,
		TimeAlias:    query.resolveTimeColumnAlias(),
		MetricAlias:  query.resolveMetricColumnAlias(),
	}.ExpandMacros(ctx, query.Code)
	if err != nil {
		if err != nil {
			return pinotlib.SqlQuery{}, err
		}
	}

	return pinotlib.NewSqlQuery(sql), nil
}

func (query PinotQlCodeQuery) ExtractResults(results *pinotlib.ResultTable) (*data.Frame, error) {
	switch query.DisplayType {
	case DisplayTypeTable, DisplayTypeAnnotations:
		return ExtractTableDataFrame(results, query.resolveTimeColumnAlias()), nil
	case DisplayTypeLogs:
		return ExtractLogsDataFrame(results, query.resolveTimeColumnAlias(), query.resolveLogColumnAlias())
	default:
		return ExtractTimeSeriesDataFrame(TimeSeriesExtractorParams{
			MetricName:        query.resolveMetricColumnAlias(),
			Legend:            query.Legend,
			TimeColumnAlias:   query.resolveTimeColumnAlias(),
			MetricColumnAlias: query.resolveMetricColumnAlias(),
			TimeColumnFormat:  OutputTimeFormat(),
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

func getOrFallback[T ~string](value T, fallback T) T {
	if value != "" {
		return value
	} else {
		return fallback
	}
}
