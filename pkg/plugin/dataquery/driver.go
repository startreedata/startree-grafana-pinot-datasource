package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
)

type Driver interface {
	Execute(ctx context.Context) backend.DataResponse
}

func NewDriver(pinotClient *pinotlib.PinotClient, query PinotDataQuery, timeRange backend.TimeRange) (Driver, error) {
	if query.Hide {
		return new(NoOpDriver), nil
	}

	switch query.QueryType {
	case QueryTypePinotQl:
		return newPinotQlDriver(pinotClient, query, timeRange)
	case QueryTypePromQl:
		return NewPromQlCodeDriver(PromQlCodeDriverParams{
			PinotClient:  pinotClient,
			TableName:    query.TableName,
			PromQlCode:   query.PromQlCode,
			TimeRange:    TimeRange{To: timeRange.To, From: timeRange.From},
			IntervalSize: query.IntervalSize,
			Legend:       query.Legend,
		}), nil
	case QueryTypePinotVariableQuery:
		return NewPinotVariableQueryDriver(PinotVariableQueryParams{
			PinotClient:  pinotClient,
			TableName:    query.TableName,
			VariableType: query.VariableQuery.VariableType,
			ColumnName:   query.VariableQuery.ColumnName,
			PinotQlCode:  query.VariableQuery.PinotQlCode,
			ColumnType:   query.VariableQuery.ColumnType,
		}), nil
	}
	return new(NoOpDriver), nil
}

func newPinotQlDriver(pinotClient *pinotlib.PinotClient, query PinotDataQuery, timeRange backend.TimeRange) (Driver, error) {
	if query.TableName == "" {
		// Don't return an error when a user first lands on the query editor.
		return new(NoOpDriver), nil
	}

	tableSchema, err := pinotClient.GetTableSchema(context.Background(), query.TableName)
	if err != nil {
		return nil, err
	}

	switch query.EditorMode {
	case EditorModeBuilder:
		return NewPinotQlBuilderDriver(pinotClient, PinotQlBuilderParams{
			TimeRange:           TimeRange{To: timeRange.To, From: timeRange.From},
			IntervalSize:        query.IntervalSize,
			TableName:           query.TableName,
			TimeColumn:          query.TimeColumn,
			MetricColumn:        query.MetricColumn,
			GroupByColumns:      query.GroupByColumns,
			AggregationFunction: query.AggregationFunction,
			DimensionFilters:    query.DimensionFilters,
			Limit:               query.Limit,
			Granularity:         query.Granularity,
			OrderByClauses:      query.OrderByClauses,
			QueryOptions:        query.QueryOptions,
			Legend:              query.Legend,
		})
	case EditorModeCode:
		if query.PinotQlCode == "" {
			return new(NoOpDriver), nil
		}
		return NewPinotQlCodeDriver(PinotQlCodeDriverParams{
			PinotClient:       pinotClient,
			Code:              query.PinotQlCode,
			TableName:         query.TableName,
			TimeColumnAlias:   query.TimeColumnAlias,
			MetricColumnAlias: query.MetricColumnAlias,
			LogColumnAlias:    query.LogColumnAlias,
			TimeRange:         TimeRange{To: timeRange.To, From: timeRange.From},
			IntervalSize:      query.IntervalSize,
			TableSchema:       tableSchema,
			DisplayType:       query.DisplayType,
			Legend:            query.Legend,
		})
	default:
		return new(NoOpDriver), nil
	}
}

type DriverFunc func(ctx context.Context) backend.DataResponse

func (f DriverFunc) Execute(ctx context.Context) backend.DataResponse { return f(ctx) }

var _ Driver = &NoOpDriver{}

type NoOpDriver struct{}

func (d *NoOpDriver) Execute(ctx context.Context) backend.DataResponse {
	return NewEmptyDataResponse()
}

func doSqlQuery(ctx context.Context, pinotClient *pinotlib.PinotClient, query pinotlib.SqlQuery) (*pinotlib.ResultTable, []pinotlib.BrokerException, bool, backend.DataResponse) {
	resp, err := pinotClient.ExecuteSqlQuery(ctx, query)
	if err != nil {
		return nil, nil, false, NewPluginErrorResponse(err)
	} else if resp.HasData() {
		return resp.ResultTable, resp.Exceptions, true, backend.DataResponse{}
	} else if resp.HasExceptions() {
		return nil, resp.Exceptions, false, NewPinotExceptionsDataResponse(resp.Exceptions)
	} else {
		return nil, nil, false, NewEmptyDataResponse()
	}
}
