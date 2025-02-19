package dataquery

import (
	"context"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"time"
)

const (
	BuilderTimeColumn   = "__time"
	BuilderMetricColumn = "__metric"
	BuilderLogColumn    = "__message"

	AggregationFunctionCount = "COUNT"
	AggregationFunctionNone  = "NONE"

	DefaultLimit = 100_000
)

var _ ExecutableQuery = TimeSeriesBuilderQuery{}

type TimeSeriesBuilderQuery struct {
	TimeRange           TimeRange
	IntervalSize        time.Duration
	TableName           string
	TimeColumn          string
	MetricColumn        ComplexField
	GroupByColumns      []ComplexField
	AggregationFunction string
	DimensionFilters    []DimensionFilter
	Limit               int64
	Granularity         string
	MaxDataPoints       int64
	OrderByClauses      []OrderByClause
	QueryOptions        []QueryOption
	Legend              string
}

func (query TimeSeriesBuilderQuery) Execute(client *pinotlib.PinotClient, ctx context.Context) backend.DataResponse {
	if err := query.Validate(); err != nil {
		return NewBadRequestErrorResponse(err)
	}

	sqlQuery, outputTimeFormat, err := query.RenderSqlQuery(ctx, client)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, client, sqlQuery)
	if !ok {
		return backendResp
	}

	frame, err := query.ExtractResults(results, outputTimeFormat)
	return NewSqlQueryDataResponse(frame, exceptions)
}

func (query TimeSeriesBuilderQuery) Validate() error {
	switch {
	case query.TableName == "":
		return errors.New("TableName is required")
	case query.TimeColumn == "":
		return errors.New("TimeColumn is required")
	case query.MetricColumn.Name == "" && query.AggregationFunction != AggregationFunctionCount:
		return errors.New("MetricColumn is required")
	case query.AggregationFunction == "":
		return errors.New("AggregationFunction is required")
	default:
		return nil
	}
}

func (query TimeSeriesBuilderQuery) RenderSqlQuery(ctx context.Context, client *pinotlib.PinotClient) (pinotlib.SqlQuery, pinotlib.DateTimeFormat, error) {
	schema, err := client.GetTableSchema(ctx, query.TableName)
	if err != nil {
		return pinotlib.SqlQuery{}, pinotlib.DateTimeFormat{}, err
	}

	tableConfigs, err := client.ListTableConfigs(ctx, query.TableName)
	if err != nil {
		return pinotlib.SqlQuery{}, pinotlib.DateTimeFormat{}, err
	}

	inputTimeFormat, err := pinotlib.GetTimeColumnFormat(schema, query.TimeColumn)
	if err != nil {
		return pinotlib.SqlQuery{}, pinotlib.DateTimeFormat{}, err
	}

	var outputTimeFormat pinotlib.DateTimeFormat
	var sql string
	if query.AggregationFunction == AggregationFunctionNone {
		outputTimeFormat = inputTimeFormat
		sql, err = pinotlib.RenderSingleMetricSql(pinotlib.SingleMetricSqlParams{
			TableNameExpr:         pinotlib.ObjectExpr(query.TableName),
			TimeColumn:            query.TimeColumn,
			MetricColumnExpr:      query.metricExpr(),
			TimeColumnAliasExpr:   pinotlib.ObjectExpr(BuilderTimeColumn),
			MetricColumnAliasExpr: pinotlib.ObjectExpr(BuilderMetricColumn),
			DimensionFilterExprs:  FilterExprsFrom(query.DimensionFilters),
			Limit:                 query.resolveLimit(),
			TimeFilterExpr: pinotlib.TimeFilterExpr(pinotlib.TimeFilter{
				Column: query.TimeColumn,
				Format: inputTimeFormat,
				From:   query.TimeRange.From,
				To:     query.TimeRange.To,
			}),
		})
	} else {
		outputTimeFormat = OutputTimeFormat()
		derivedGranularities := pinotlib.DerivedGranularitiesFor(tableConfigs, query.TimeColumn, outputTimeFormat)
		granularity := ResolveGranularity(ctx, query.Granularity, inputTimeFormat, query.IntervalSize, derivedGranularities)
		timeGroup := timeGroupOf(query.TimeColumn, inputTimeFormat, granularity)
		sql, err = pinotlib.RenderTimeSeriesSql(pinotlib.TimeSeriesSqlParams{
			TableNameExpr:         pinotlib.ObjectExpr(query.TableName),
			TimeGroupExpr:         pinotlib.TimeGroupExpr(tableConfigs, timeGroup),
			MetricColumnExpr:      query.metricExpr(),
			TimeColumnAliasExpr:   pinotlib.ObjectExpr(BuilderTimeColumn),
			MetricColumnAliasExpr: pinotlib.ObjectExpr(BuilderMetricColumn),
			AggregationFunction:   query.AggregationFunction,
			GroupByColumnExprs:    query.groupByExprs(),
			DimensionFilterExprs:  FilterExprsFrom(query.DimensionFilters),
			Limit:                 query.resolveLimit(),
			OrderByExprs:          OrderByExprs(query.OrderByClauses),
			TimeFilterExpr: pinotlib.TimeFilterBucketAlignedExpr(pinotlib.TimeFilter{
				Column: query.TimeColumn,
				Format: timeGroup.InputFormat,
				From:   query.TimeRange.From,
				To:     query.TimeRange.To,
			}, timeGroup.Granularity.Duration()),
		})
	}
	if err != nil {
		return pinotlib.SqlQuery{}, pinotlib.DateTimeFormat{}, err
	}

	return newSqlQueryWithOptions(sql, query.QueryOptions), outputTimeFormat, nil
}

func (query TimeSeriesBuilderQuery) RenderSqlWithMacros() (string, error) {
	var sql string
	var err error

	if query.AggregationFunction == AggregationFunctionNone {
		sql, err = pinotlib.RenderSingleMetricSql(pinotlib.SingleMetricSqlParams{
			TableNameExpr:         MacroExprFor(MacroTable),
			TimeColumn:            query.TimeColumn,
			TimeColumnAliasExpr:   MacroExprFor(MacroTimeAlias),
			MetricColumnExpr:      pinotlib.ComplexFieldExpr(query.MetricColumn.Name, query.MetricColumn.Key),
			MetricColumnAliasExpr: MacroExprFor(MacroMetricAlias),
			TimeFilterExpr:        MacroExprFor(MacroTimeFilter, pinotlib.ObjectExpr(query.TimeColumn).String()),
			DimensionFilterExprs:  FilterExprsFrom(query.DimensionFilters),
			Limit:                 query.resolveLimit(),
		})
	} else {
		timeColExpr := pinotlib.ObjectExpr(query.TimeColumn)
		granularityExpr := pinotlib.LiteralExpr(getOrFallback(query.Granularity, "auto"))
		sql, err = pinotlib.RenderTimeSeriesSql(pinotlib.TimeSeriesSqlParams{
			TableNameExpr:         MacroExprFor(MacroTable),
			TimeGroupExpr:         MacroExprFor(MacroTimeGroup, timeColExpr.String(), granularityExpr.String()),
			TimeColumnAliasExpr:   MacroExprFor(MacroTimeAlias),
			AggregationFunction:   query.AggregationFunction,
			MetricColumnExpr:      query.metricExpr(),
			MetricColumnAliasExpr: MacroExprFor(MacroMetricAlias),
			GroupByColumnExprs:    query.groupByExprs(),
			TimeFilterExpr:        MacroExprFor(MacroTimeFilter, timeColExpr.String(), granularityExpr.String()),
			DimensionFilterExprs:  FilterExprsFrom(query.DimensionFilters),
			Limit:                 query.resolveLimit(),
			OrderByExprs:          OrderByExprs(query.OrderByClauses),
		})
	}
	if err != nil {
		return "", err
	}
	return newSqlQueryWithOptions(sql, query.QueryOptions).RenderSql(), nil
}

func (query TimeSeriesBuilderQuery) ExtractResults(results *pinotlib.ResultTable, outputTimeFormat pinotlib.DateTimeFormat) (*data.Frame, error) {
	return ExtractTimeSeriesDataFrame(TimeSeriesExtractorParams{
		MetricName:        query.resolveMetricName(),
		Legend:            query.Legend,
		MetricColumnAlias: BuilderMetricColumn,
		TimeColumnAlias:   BuilderTimeColumn,
		TimeColumnFormat:  outputTimeFormat,
	}, results)
}

func (query TimeSeriesBuilderQuery) resolveOutputTimeFormat(tableSchema pinotlib.TableSchema) (pinotlib.DateTimeFormat, error) {
	if query.AggregationFunction == AggregationFunctionNone {
		return pinotlib.GetTimeColumnFormat(tableSchema, query.TimeColumn)
	} else {
		return OutputTimeFormat(), nil
	}
}

func (query TimeSeriesBuilderQuery) metricExpr() pinotlib.SqlExpr {
	if query.AggregationFunction == AggregationFunctionCount {
		return pinotlib.ObjectExpr("*")
	} else {
		return pinotlib.ComplexFieldExpr(query.MetricColumn.Name, query.MetricColumn.Key)
	}
}

func (query TimeSeriesBuilderQuery) resolveMetricName() string {
	switch {
	case query.AggregationFunction == AggregationFunctionCount:
		return "count"
	case query.MetricColumn.Key == "":
		return query.MetricColumn.Name
	default:
		return complexFieldAlias(query.MetricColumn.Name, query.MetricColumn.Key)
	}
}

func (query TimeSeriesBuilderQuery) resolveLimit() int64 {
	switch true {
	case query.Limit >= 1:
		return query.Limit
	case query.AggregationFunction != AggregationFunctionNone && len(query.GroupByColumns) > 0:
		// Use default limit for group by queries.
		return DefaultLimit
	case query.MaxDataPoints > 0:
		return query.MaxDataPoints
	default:
		return DefaultLimit
	}
}

func (query TimeSeriesBuilderQuery) groupByExprs() []pinotlib.ExprWithAlias {
	var exprs []pinotlib.ExprWithAlias
	for _, col := range query.GroupByColumns {
		if col.Name != "" {
			exprs = append(exprs, pinotlib.ExprWithAlias{
				Expr:  pinotlib.ComplexFieldExpr(col.Name, col.Key),
				Alias: complexFieldAlias(col.Name, col.Key),
			})
		}
	}
	return exprs
}

func timeGroupOf(timeColumn string, timeColumnFormat pinotlib.DateTimeFormat, granularity pinotlib.Granularity) pinotlib.DateTimeConversion {
	timeGroup := pinotlib.DateTimeConversion{
		TimeColumn:   timeColumn,
		InputFormat:  timeColumnFormat,
		OutputFormat: OutputTimeFormat(),
		Granularity:  granularity,
	}
	return timeGroup
}

func complexFieldAlias(name string, key string) string {
	if key == "" {
		return ""
	} else {
		return fmt.Sprintf(`%s[%s]`, name, key)
	}
}
