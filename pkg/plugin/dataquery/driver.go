package dataquery

import (
	"context"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
)

type Driver interface {
	Execute(ctx context.Context) backend.DataResponse
}

func NewDriver(ctx context.Context, pinotClient *pinotlib.PinotClient, query PinotDataQuery, timeRange backend.TimeRange) (Driver, error) {
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
		return NewPinotQlBuilderDriver(PinotQlBuilderParams{
			PinotClient:         pinotClient,
			TableSchema:         tableSchema,
			TimeRange:           TimeRange{To: timeRange.To, From: timeRange.From},
			IntervalSize:        query.IntervalSize,
			DatabaseName:        query.DatabaseName,
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
			DatabaseName:      query.DatabaseName,
			TableName:         query.TableName,
			TimeColumnAlias:   query.TimeColumnAlias,
			TimeColumnFormat:  query.TimeColumnFormat,
			MetricColumnAlias: query.MetricColumnAlias,
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

var _ Driver = &NoOpDriver{}

type NoOpDriver struct{}

func (d *NoOpDriver) Execute(ctx context.Context) backend.DataResponse {
	return backend.DataResponse{}
}

func NewDataResponse(frames ...*data.Frame) backend.DataResponse {
	return backend.DataResponse{Frames: frames}
}

func NewDataInternalErrorResponse(err error) backend.DataResponse {
	return NewDataErrorResponse(backend.StatusInternal, err)
}

func NewDataErrorResponse(status backend.Status, err error) backend.DataResponse {
	return backend.ErrDataResponse(status, fmt.Sprintf("Error: %s.", err.Error()))
}
