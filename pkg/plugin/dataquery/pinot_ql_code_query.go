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

func (query PinotQlCodeQuery) Execute(client *pinot.Client, ctx context.Context) backend.DataResponse {
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

func (query PinotQlCodeQuery) RenderSqlQuery(ctx context.Context, client *pinot.Client) (pinot.SqlQuery, error) {
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
		TimeRange:    query.TimeRange,
		IntervalSize: query.IntervalSize,
		TimeAlias:    query.resolveTimeColumnAlias(),
		MetricAlias:  query.resolveMetricColumnAlias(),
	}.ExpandMacros(ctx, query.Code)
	if err != nil {
		if err != nil {
			return pinot.SqlQuery{}, err
		}
	}

	return pinot.NewSqlQuery(sql), nil
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

func getOrFallback[T ~string](value T, fallback T) T {
	if value != "" {
		return value
	} else {
		return fallback
	}
}
