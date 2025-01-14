package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
)

func ExecutableQueryFrom(query PinotDataQuery) ExecutableQuery {
	switch {
	case query.Hide:
		return new(NoOpQuery)

	case query.QueryType == QueryTypePromQl:
		return PromQlQuery{
			TableName:    query.TableName,
			PromQlCode:   query.PromQlCode,
			TimeRange:    query.TimeRange,
			IntervalSize: query.IntervalSize,
			Legend:       query.Legend,
		}

	case query.QueryType == QueryTypePinotVariableQuery:
		return VariableQuery{
			TableName:    query.TableName,
			VariableType: query.VariableQuery.VariableType,
			ColumnName:   query.VariableQuery.ColumnName,
			PinotQlCode:  query.VariableQuery.PinotQlCode,
			ColumnType:   query.VariableQuery.ColumnType,
		}

	case query.QueryType == QueryTypePinotQl && query.EditorMode == EditorModeCode:
		return PinotQlCodeQuery{
			Code:              query.PinotQlCode,
			TableName:         query.TableName,
			TimeColumnAlias:   query.TimeColumnAlias,
			MetricColumnAlias: query.MetricColumnAlias,
			LogColumnAlias:    query.LogColumnAlias,
			TimeRange:         query.TimeRange,
			IntervalSize:      query.IntervalSize,
			DisplayType:       query.DisplayType,
			Legend:            query.Legend,
		}

	case query.QueryType == QueryTypePinotQl && query.EditorMode == EditorModeBuilder && query.DisplayType == DisplayTypeLogs:
		return LogsBuilderQuery{
			TimeRange:        query.TimeRange,
			TableName:        query.TableName,
			TimeColumn:       query.TimeColumn,
			LogColumn:        query.LogColumn,
			LogColumnAlias:   query.LogColumnAlias,
			MetadataColumns:  query.MetadataColumns,
			JsonExtractors:   query.JsonExtractors,
			RegexpExtractors: query.RegexpExtractors,
			DimensionFilters: query.DimensionFilters,
			QueryOptions:     query.QueryOptions,
			Limit:            query.Limit,
		}

	case query.QueryType == QueryTypePinotQl && query.EditorMode == EditorModeBuilder:
		return TimeSeriesBuilderQuery{
			TimeRange:           query.TimeRange,
			IntervalSize:        query.IntervalSize,
			TableName:           query.TableName,
			TimeColumn:          query.TimeColumn,
			MetricColumn:        builderMetricColumnFrom(query),
			GroupByColumns:      builderGroupByColumnsFrom(query),
			AggregationFunction: query.AggregationFunction,
			DimensionFilters:    query.DimensionFilters,
			Limit:               query.Limit,
			Granularity:         query.Granularity,
			OrderByClauses:      query.OrderByClauses,
			QueryOptions:        query.QueryOptions,
			Legend:              query.Legend,
		}

	default:
		return new(NoOpQuery)
	}
}

type ExecutableQuery interface {
	Execute(ctx context.Context, client *pinotlib.PinotClient) backend.DataResponse
}

var _ ExecutableQuery = NoOpQuery{}

type NoOpQuery struct{}

func (d NoOpQuery) Execute(context.Context, *pinotlib.PinotClient) backend.DataResponse {
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
