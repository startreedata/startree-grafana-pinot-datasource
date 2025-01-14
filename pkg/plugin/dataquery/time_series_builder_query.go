package dataquery

import (
	"context"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/templates"
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

func (query TimeSeriesBuilderQuery) Execute(ctx context.Context, client *pinotlib.PinotClient) backend.DataResponse {
	if err := query.Validate(); err != nil {
		return NewPluginErrorResponse(err)
	}

	tableSchema, err := client.GetTableSchema(ctx, query.TableName)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	tableConfigs, err := client.ListTableConfigs(ctx, query.TableName)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	sql, err := query.RenderSql(ctx, tableSchema, tableConfigs)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, client, pinotlib.NewSqlQuery(sql))
	if !ok {
		return backendResp
	}

	frame, err := query.ExtractResults(results, tableSchema)
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

func (query TimeSeriesBuilderQuery) RenderSql(ctx context.Context, schema pinotlib.TableSchema, tableConfigs pinotlib.ListTableConfigsResponse) (string, error) {
	timeColumnFormat, err := pinotlib.GetTimeColumnFormat(schema, query.TimeColumn)
	if err != nil {
		return "", err
	}

	if query.AggregationFunction == AggregationFunctionNone {
		return templates.RenderSingleMetricSql(templates.SingleMetricSqlParams{
			TableNameExpr:         pinotlib.ObjectExpr(query.TableName),
			TimeColumn:            query.TimeColumn,
			MetricColumnExpr:      query.metricExpr(),
			TimeColumnAliasExpr:   pinotlib.ObjectExpr(BuilderTimeColumn),
			MetricColumnAliasExpr: pinotlib.ObjectExpr(BuilderMetricColumn),
			DimensionFilterExprs:  FilterExprsFrom(query.DimensionFilters),
			Limit:                 query.resolveLimit(),
			QueryOptionsExpr:      QueryOptionsExpr(query.QueryOptions),
			TimeFilterExpr: pinotlib.TimeFilterExpr(pinotlib.TimeFilter{
				Column: query.TimeColumn,
				Format: timeColumnFormat,
				From:   query.TimeRange.From,
				To:     query.TimeRange.To,
			}),
		})
	}

	derivedGranularities := pinotlib.DerivedGranularitiesFor(tableConfigs, query.TimeColumn, OutputTimeFormat())
	granularity := ResolveGranularity(ctx, query.Granularity, timeColumnFormat, query.IntervalSize, derivedGranularities)
	timeGroup := timeGroupOf(query.TimeColumn, timeColumnFormat, granularity)

	return templates.RenderTimeSeriesSql(templates.TimeSeriesSqlParams{
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
		QueryOptionsExpr:      QueryOptionsExpr(query.QueryOptions),
		TimeFilterExpr: pinotlib.TimeFilterBucketAlignedExpr(pinotlib.TimeFilter{
			Column: query.TimeColumn,
			Format: timeGroup.InputFormat,
			From:   query.TimeRange.From,
			To:     query.TimeRange.To,
		}, timeGroup.Granularity.Duration()),
	})
}

func (query TimeSeriesBuilderQuery) RenderSqlWithMacros(ctx context.Context, schema pinotlib.TableSchema, tableConfigs pinotlib.ListTableConfigsResponse) (string, error) {
	timeColumnFormat, err := pinotlib.GetTimeColumnFormat(schema, query.TimeColumn)
	if err != nil {
		return "", err
	}

	if query.AggregationFunction == AggregationFunctionNone {
		return templates.RenderSingleMetricSql(templates.SingleMetricSqlParams{
			TableNameExpr:         MacroExprFor(MacroTable),
			TimeColumn:            query.TimeColumn,
			TimeColumnAliasExpr:   MacroExprFor(MacroTimeAlias),
			MetricColumnExpr:      pinotlib.ComplexFieldExpr(query.MetricColumn.Name, query.MetricColumn.Key),
			MetricColumnAliasExpr: MacroExprFor(MacroMetricAlias),
			TimeFilterExpr:        MacroExprFor(MacroTimeFilter, pinotlib.ObjectExpr(query.TimeColumn)),
			DimensionFilterExprs:  FilterExprsFrom(query.DimensionFilters),
			Limit:                 query.resolveLimit(),
			QueryOptionsExpr:      QueryOptionsExpr(query.QueryOptions),
		})
	}

	derivedGranularities := pinotlib.DerivedGranularitiesFor(tableConfigs, query.TimeColumn, OutputTimeFormat())
	granularity := ResolveGranularity(ctx, query.Granularity, timeColumnFormat, query.IntervalSize, derivedGranularities)
	timeGroup := timeGroupOf(query.TimeColumn, timeColumnFormat, granularity)
	return templates.RenderTimeSeriesSql(templates.TimeSeriesSqlParams{
		TableNameExpr:         MacroExprFor(MacroTable),
		TimeGroupExpr:         MacroExprFor(MacroTimeGroup, pinotlib.ObjectExpr(query.TimeColumn), pinotlib.GranularityExpr(timeGroup.Granularity)),
		TimeColumnAliasExpr:   MacroExprFor(MacroTimeAlias),
		AggregationFunction:   query.AggregationFunction,
		MetricColumnExpr:      query.metricExpr(),
		MetricColumnAliasExpr: MacroExprFor(MacroMetricAlias),
		GroupByColumnExprs:    query.groupByExprs(),
		TimeFilterExpr:        MacroExprFor(MacroTimeFilter, pinotlib.ObjectExpr(query.TimeColumn), pinotlib.GranularityExpr(timeGroup.Granularity)),
		DimensionFilterExprs:  FilterExprsFrom(query.DimensionFilters),
		Limit:                 query.resolveLimit(),
		OrderByExprs:          OrderByExprs(query.OrderByClauses),
		QueryOptionsExpr:      QueryOptionsExpr(query.QueryOptions),
	})
}

func (query TimeSeriesBuilderQuery) ExtractResults(results *pinotlib.ResultTable, tableSchema pinotlib.TableSchema) (*data.Frame, error) {
	outputTimeFormat, err := query.resolveOutputTimeFormat(tableSchema)
	if err != nil {
		return nil, err
	}

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

func (query TimeSeriesBuilderQuery) metricExpr() string {
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

func (query TimeSeriesBuilderQuery) groupByExprs() []templates.ExprWithAlias {
	var exprs []templates.ExprWithAlias
	for _, col := range query.GroupByColumns {
		if col.Name != "" {
			exprs = append(exprs, templates.ExprWithAlias{
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
