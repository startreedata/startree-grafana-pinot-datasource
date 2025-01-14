package dataquery

import (
	"context"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
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

type TimeSeriesBuilderParams struct {
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

func (params TimeSeriesBuilderParams) Validate() error {
	switch {
	case params.TableName == "":
		return errors.New("TableName is required")
	case params.TimeColumn == "":
		return errors.New("TimeColumn is required")
	case params.MetricColumn.Name == "" && params.AggregationFunction != AggregationFunctionCount:
		return errors.New("MetricColumn is required")
	case params.AggregationFunction == "":
		return errors.New("AggregationFunction is required")
	default:
		return nil
	}
}

func ExecuteTimeSeriesBuilderQuery(ctx context.Context, client *pinotlib.PinotClient, params TimeSeriesBuilderParams) backend.DataResponse {
	if err := params.Validate(); err != nil {
		return NewPluginErrorResponse(err)
	}

	tableSchema, err := client.GetTableSchema(ctx, params.TableName)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	tableConfigs, err := client.ListTableConfigs(ctx, params.TableName)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	sql, err := RenderTimeSeriesSql(ctx, params, tableSchema, tableConfigs)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, client, pinotlib.NewSqlQuery(sql))
	if !ok {
		return backendResp
	}

	var outputTimeFormat pinotlib.DateTimeFormat
	if params.AggregationFunction == AggregationFunctionNone {
		outputTimeFormat, err = pinotlib.GetTimeColumnFormat(tableSchema, params.TimeColumn)
	} else {
		outputTimeFormat = OutputTimeFormat()
	}
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	frame, err := ExtractTimeSeriesDataFrame(TimeSeriesExtractorParams{
		MetricName:        resolveMetricName(params.MetricColumn, params.AggregationFunction),
		Legend:            params.Legend,
		MetricColumnAlias: BuilderMetricColumn,
		TimeColumnAlias:   BuilderTimeColumn,
		TimeColumnFormat:  outputTimeFormat,
	}, results)
	if err != nil {
		return NewPluginErrorResponse(err)
	}
	return NewSqlQueryDataResponse(frame, exceptions)
}

func RenderTimeSeriesSql(ctx context.Context, params TimeSeriesBuilderParams, schema pinotlib.TableSchema, tableConfigs pinotlib.ListTableConfigsResponse) (string, error) {
	timeColumnFormat, err := pinotlib.GetTimeColumnFormat(schema, params.TimeColumn)
	if err != nil {
		return "", err
	}

	if params.AggregationFunction == AggregationFunctionNone {
		return templates.RenderSingleMetricSql(templates.SingleMetricSqlParams{
			TableNameExpr:         pinotlib.ObjectExpr(params.TableName),
			TimeColumn:            params.TimeColumn,
			MetricColumnExpr:      resolveMetricExpr(params.MetricColumn, params.AggregationFunction),
			TimeColumnAliasExpr:   pinotlib.ObjectExpr(BuilderTimeColumn),
			MetricColumnAliasExpr: pinotlib.ObjectExpr(BuilderMetricColumn),
			DimensionFilterExprs:  FilterExprsFrom(params.DimensionFilters),
			Limit:                 resolveLimit(params),
			QueryOptionsExpr:      QueryOptionsExpr(params.QueryOptions),
			TimeFilterExpr: pinotlib.TimeFilterExpr(pinotlib.TimeFilter{
				Column: params.TimeColumn,
				Format: timeColumnFormat,
				From:   params.TimeRange.From,
				To:     params.TimeRange.To,
			}),
		})
	}

	derivedGranularities := pinotlib.DerivedGranularitiesFor(tableConfigs, params.TimeColumn, OutputTimeFormat())
	granularity := ResolveGranularity(ctx, params.Granularity, timeColumnFormat, params.IntervalSize, derivedGranularities)
	timeGroup := timeGroupOf(params.TimeColumn, timeColumnFormat, granularity)

	return templates.RenderTimeSeriesSql(templates.TimeSeriesSqlParams{
		TableNameExpr:         pinotlib.ObjectExpr(params.TableName),
		TimeGroupExpr:         pinotlib.TimeGroupExpr(tableConfigs, timeGroup),
		MetricColumnExpr:      resolveMetricExpr(params.MetricColumn, params.AggregationFunction),
		TimeColumnAliasExpr:   pinotlib.ObjectExpr(BuilderTimeColumn),
		MetricColumnAliasExpr: pinotlib.ObjectExpr(BuilderMetricColumn),
		AggregationFunction:   params.AggregationFunction,
		GroupByColumnExprs:    groupByExprsOf(params),
		DimensionFilterExprs:  FilterExprsFrom(params.DimensionFilters),
		Limit:                 resolveLimit(params),
		OrderByExprs:          OrderByExprs(params.OrderByClauses),
		QueryOptionsExpr:      QueryOptionsExpr(params.QueryOptions),
		TimeFilterExpr: pinotlib.TimeFilterBucketAlignedExpr(pinotlib.TimeFilter{
			Column: params.TimeColumn,
			Format: timeGroup.InputFormat,
			From:   params.TimeRange.From,
			To:     params.TimeRange.To,
		}, timeGroup.Granularity.Duration()),
	})
}

func RenderTimeSeriesSqlWithMacros(ctx context.Context, params TimeSeriesBuilderParams, schema pinotlib.TableSchema, tableConfigs pinotlib.ListTableConfigsResponse) (string, error) {
	timeColumnFormat, err := pinotlib.GetTimeColumnFormat(schema, params.TimeColumn)
	if err != nil {
		return "", err
	}

	if params.AggregationFunction == AggregationFunctionNone {
		return templates.RenderSingleMetricSql(templates.SingleMetricSqlParams{
			TableNameExpr:         MacroExprFor(MacroTable),
			TimeColumn:            params.TimeColumn,
			TimeColumnAliasExpr:   MacroExprFor(MacroTimeAlias),
			MetricColumnExpr:      pinotlib.ComplexFieldExpr(params.MetricColumn.Name, params.MetricColumn.Key),
			MetricColumnAliasExpr: MacroExprFor(MacroMetricAlias),
			TimeFilterExpr:        MacroExprFor(MacroTimeFilter, pinotlib.ObjectExpr(params.TimeColumn)),
			DimensionFilterExprs:  FilterExprsFrom(params.DimensionFilters),
			Limit:                 resolveLimit(params),
			QueryOptionsExpr:      QueryOptionsExpr(params.QueryOptions),
		})
	}

	derivedGranularities := pinotlib.DerivedGranularitiesFor(tableConfigs, params.TimeColumn, OutputTimeFormat())
	granularity := ResolveGranularity(ctx, params.Granularity, timeColumnFormat, params.IntervalSize, derivedGranularities)
	timeGroup := timeGroupOf(params.TimeColumn, timeColumnFormat, granularity)
	return templates.RenderTimeSeriesSql(templates.TimeSeriesSqlParams{
		TableNameExpr:         MacroExprFor(MacroTable),
		TimeGroupExpr:         MacroExprFor(MacroTimeGroup, pinotlib.ObjectExpr(params.TimeColumn), pinotlib.GranularityExpr(timeGroup.Granularity)),
		TimeColumnAliasExpr:   MacroExprFor(MacroTimeAlias),
		AggregationFunction:   params.AggregationFunction,
		MetricColumnExpr:      resolveMetricExpr(params.MetricColumn, params.AggregationFunction),
		MetricColumnAliasExpr: MacroExprFor(MacroMetricAlias),
		GroupByColumnExprs:    groupByExprsOf(params),
		TimeFilterExpr:        MacroExprFor(MacroTimeFilter, pinotlib.ObjectExpr(params.TimeColumn), pinotlib.GranularityExpr(timeGroup.Granularity)),
		DimensionFilterExprs:  FilterExprsFrom(params.DimensionFilters),
		Limit:                 resolveLimit(params),
		OrderByExprs:          OrderByExprs(params.OrderByClauses),
		QueryOptionsExpr:      QueryOptionsExpr(params.QueryOptions),
	})
}

func complexFieldAlias(name string, key string) string {
	if key == "" {
		return ""
	} else {
		return fmt.Sprintf(`%s[%s]`, name, key)
	}
}

func FilterExprsFrom(filters []DimensionFilter) []string {
	exprs := make([]string, 0, len(filters))
	for _, filter := range filters {
		expr := pinotlib.ColumnFilterExpr(pinotlib.ColumnFilter{
			ColumnName: filter.ColumnName,
			ColumnKey:  filter.ColumnKey,
			ValueExprs: filter.ValueExprs,
			Operator:   pinotlib.FilterOperator(filter.Operator),
		})
		if expr == "" {
			continue
		}
		exprs = append(exprs, expr)
	}
	return exprs[:]
}

func resolveMetricExpr(metricColumn ComplexField, aggregationFunction string) string {
	if aggregationFunction == AggregationFunctionCount {
		return pinotlib.ObjectExpr("*")
	} else {
		return pinotlib.ComplexFieldExpr(metricColumn.Name, metricColumn.Key)
	}
}

func resolveMetricName(metricColumn ComplexField, aggregationFunction string) string {
	switch {
	case aggregationFunction == AggregationFunctionCount:
		return "count"
	case metricColumn.Key == "":
		return metricColumn.Name
	default:
		return complexFieldAlias(metricColumn.Name, metricColumn.Key)
	}
}

func resolveLimit(params TimeSeriesBuilderParams) int64 {
	switch true {
	case params.Limit >= 1:
		// Use provided limit if present
		return params.Limit
	case params.AggregationFunction != AggregationFunctionNone && len(params.GroupByColumns) > 0:
		// Use default limit for group by queries.
		// TODO: Resolve more accurate limit in this case.
		return DefaultLimit
	case params.MaxDataPoints > 0:
		// Queries with extra dimensions can directly use max data points.
		return params.MaxDataPoints
	default:
		return DefaultLimit
	}
}

func groupByExprsOf(params TimeSeriesBuilderParams) []templates.ExprWithAlias {
	var exprs []templates.ExprWithAlias
	for _, col := range params.GroupByColumns {
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
