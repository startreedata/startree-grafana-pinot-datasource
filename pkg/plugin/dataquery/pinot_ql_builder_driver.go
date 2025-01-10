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
	DefaultTimeColumnAlias   = "time"
	DefaultMetricColumnAlias = "metric"
	DefaultLogColumnAlias    = "message"

	AggregationFunctionCount = "COUNT"
	AggregationFunctionNone  = "NONE"

	DefaultLimit = 100_000
)

type PinotQlBuilderDriver struct {
	params            PinotQlBuilderParams
	TimeColumnAlias   string
	MetricColumnAlias string
	TimeGroup         pinotlib.DateTimeConversion
	TableConfigs      pinotlib.ListTableConfigsResponse
}

type PinotQlBuilderParams struct {
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

func (params PinotQlBuilderParams) Validate() error {
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

func ExecuteTimeSeriesBuilderQuery(ctx context.Context, client *pinotlib.PinotClient, params PinotQlBuilderParams) backend.DataResponse {
	if err := params.Validate(); err != nil {
		return NewPluginErrorResponse(err)
	}

	tableSchema, err := client.GetTableSchema(ctx, params.TableName)
	if err != nil {
		return NewDownstreamErrorResponse(err)
	}

	tableConfigs, err := client.ListTableConfigs(ctx, params.TableName)
	if err != nil {
		return NewDownstreamErrorResponse(err)
	}

	sql, err := RenderTimeSeriesBuilderSqlAgg(ctx, tableSchema, tableConfigs, params)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, client, pinotlib.NewSqlQuery(sql))
	if !ok {
		return backendResp
	}

	frame, err := ExtractTimeSeriesDataFrame(results, params)
	if err != nil {
		return NewPluginErrorResponse(err)
	}
	return NewSqlQueryDataResponse(frame, exceptions)

}

func ExtractTimeSeriesDataFrame(results *pinotlib.ResultTable, params PinotQlBuilderParams) (*data.Frame, error) {
	timeColumnFormat, err := pinotlib.GetTimeColumnFormat(schema, params.TimeColumn)
	if err != nil {
		return "", err
	}

	return ExtractTimeSeriesDataFrame(TimeSeriesExtractorParams{
		MetricName:        resolveMetricAlias(params.MetricColumn, params.AggregationFunction),
		Legend:            params.Legend,
		TimeColumnAlias:   params.TimeColumn,
		MetricColumnAlias: resolveMetricAlias(params.MetricColumn, params.AggregationFunction),
		TimeColumnFormat:  p.resolveTimeColumnFormat(),
	}, results)
}

func complexFieldAlias(name string, key string) string {
	if key == "" {
		return name
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

func (p *PinotQlBuilderDriver) timeGroupExpr(expandMacros bool) string {
	if expandMacros {
		return pinotlib.TimeGroupExpr(p.TableConfigs, p.TimeGroup)
	} else {
		return MacroExprFor(MacroTimeGroup, pinotlib.ObjectExpr(p.params.TimeColumn), pinotlib.GranularityExpr(p.TimeGroup.Granularity))
	}
}

func resolveMetricAlias(metricColumn ComplexField, aggregationFunction string) string {
	if aggregationFunction == AggregationFunctionCount {
		return "count"
	} else {
		return complexFieldAlias(metricColumn.Name, metricColumn.Key)
	}
}

func resolveLimit(params PinotQlBuilderParams) int64 {
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

func resolveMetricColumnExpr(metricColumn ComplexField, aggregationFunction string) string {
	if aggregationFunction == AggregationFunctionCount {
		return pinotlib.ObjectExpr("*")
	} else {
		return pinotlib.ComplexFieldExpr(metricColumn.Name, metricColumn.Key)
	}
}

func RenderTimeSeriesBuilderSqlNoAgg(schema pinotlib.TableSchema, params PinotQlBuilderParams) (string, error) {
	timeColumnFormat, err := pinotlib.GetTimeColumnFormat(schema, params.TimeColumn)
	if err != nil {
		return "", err
	}

	timeFilterExpr := pinotlib.TimeFilterExpr(pinotlib.TimeFilter{
		Column: params.TimeColumn,
		Format: timeColumnFormat,
		From:   params.TimeRange.From,
		To:     params.TimeRange.To,
	})
	return templates.RenderSingleMetricSql(templates.SingleMetricSqlParams{
		TableNameExpr:         pinotlib.ObjectExpr(params.TableName),
		TimeColumn:            params.TimeColumn,
		TimeColumnAliasExpr:   pinotlib.ObjectExpr(params.TimeColumn),
		MetricColumnExpr:      pinotlib.ComplexFieldExpr(params.MetricColumn.Name, params.MetricColumn.Key),
		MetricColumnAliasExpr: resolveMetricAlias(params.MetricColumn, params.AggregationFunction),
		TimeFilterExpr:        timeFilterExpr,
		DimensionFilterExprs:  FilterExprsFrom(params.DimensionFilters),
		Limit:                 resolveLimit(params),
		QueryOptionsExpr:      QueryOptionsExpr(params.QueryOptions),
	})
}

func RenderTimeSeriesBuilderSqlNoAggWithMacros(params PinotQlBuilderParams) (string, error) {
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

func RenderTimeSeriesBuilderSqlAgg(ctx context.Context, schema pinotlib.TableSchema, tableConfigs pinotlib.ListTableConfigsResponse, params PinotQlBuilderParams) (string, error) {
	timeColumnFormat, err := pinotlib.GetTimeColumnFormat(schema, params.TimeColumn)
	if err != nil {
		return "", err
	}

	derivedGranularities := pinotlib.DerivedGranularitiesFor(tableConfigs, params.TimeColumn, TimeOutputFormat())
	granularity := ResolveGranularity(ctx, params.Granularity, timeColumnFormat, params.IntervalSize, derivedGranularities)
	timeGroup := timeGroupOf(params.TimeColumn, timeColumnFormat, granularity)
	timeFilterExpr := pinotlib.TimeFilterBucketAlignedExpr(pinotlib.TimeFilter{
		Column: params.TimeColumn,
		Format: timeGroup.InputFormat,
		From:   params.TimeRange.From,
		To:     params.TimeRange.To,
	}, timeGroup.Granularity.Duration())
	return templates.RenderTimeSeriesSql(templates.TimeSeriesSqlParams{
		TableNameExpr:         pinotlib.ObjectExpr(params.TableName),
		TimeGroupExpr:         pinotlib.TimeGroupExpr(tableConfigs, timeGroup),
		TimeColumnAliasExpr:   pinotlib.ObjectExpr(params.TimeColumn),
		AggregationFunction:   params.AggregationFunction,
		MetricColumnExpr:      pinotlib.ComplexFieldExpr(params.MetricColumn.Name, params.MetricColumn.Key),
		MetricColumnAliasExpr: resolveMetricAlias(params.MetricColumn, params.AggregationFunction),
		GroupByColumnExprs:    groupByExprsOf(params),
		TimeFilterExpr:        timeFilterExpr,
		DimensionFilterExprs:  FilterExprsFrom(params.DimensionFilters),
		Limit:                 resolveLimit(params),
		OrderByExprs:          OrderByExprs(params.OrderByClauses),
		QueryOptionsExpr:      QueryOptionsExpr(params.QueryOptions),
	})
}

func RenderTimeSeriesBuilderSqlAggWithMacros(ctx context.Context, timeColumnFormat pinotlib.DateTimeFormat, tableConfigs pinotlib.ListTableConfigsResponse, params PinotQlBuilderParams) (string, error) {
	derivedGranularities := pinotlib.DerivedGranularitiesFor(tableConfigs, params.TimeColumn, TimeOutputFormat())
	granularity := ResolveGranularity(ctx, params.Granularity, timeColumnFormat, params.IntervalSize, derivedGranularities)
	timeGroup := timeGroupOf(params.TimeColumn, timeColumnFormat, granularity)
	return templates.RenderTimeSeriesSql(templates.TimeSeriesSqlParams{
		TableNameExpr:         MacroExprFor(MacroTable),
		TimeGroupExpr:         MacroExprFor(MacroTimeGroup, pinotlib.ObjectExpr(params.TimeColumn), pinotlib.GranularityExpr(timeGroup.Granularity)),
		TimeColumnAliasExpr:   MacroExprFor(MacroTimeAlias),
		AggregationFunction:   params.AggregationFunction,
		MetricColumnExpr:      resolveMetricColumnExpr(params.MetricColumn, params.AggregationFunction),
		MetricColumnAliasExpr: MacroExprFor(MacroMetricAlias),
		GroupByColumnExprs:    groupByExprsOf(params),
		TimeFilterExpr:        MacroExprFor(MacroTimeFilter, pinotlib.ObjectExpr(params.TimeColumn), pinotlib.GranularityExpr(timeGroup.Granularity)),
		DimensionFilterExprs:  FilterExprsFrom(params.DimensionFilters),
		Limit:                 resolveLimit(params),
		OrderByExprs:          OrderByExprs(params.OrderByClauses),
		QueryOptionsExpr:      QueryOptionsExpr(params.QueryOptions),
	})
}

func groupByExprsOf(params PinotQlBuilderParams) []templates.ExprWithAlias {
	groupByColumns := make([]templates.ExprWithAlias, len(params.GroupByColumns))
	for i, col := range params.GroupByColumns {
		if expr := pinotlib.ComplexFieldExpr(col.Name, col.Key); expr != "" {
			groupByColumns[i] = templates.ExprWithAlias{
				Expr:  pinotlib.ComplexFieldExpr(col.Name, col.Key),
				Alias: complexFieldAlias(col.Name, col.Key),
			}
		}
	}
	return groupByColumns
}

func timeGroupOf(timeColumn string, timeColumnFormat pinotlib.DateTimeFormat, granularity pinotlib.Granularity) pinotlib.DateTimeConversion {
	timeGroup := pinotlib.DateTimeConversion{
		TimeColumn:   timeColumn,
		InputFormat:  timeColumnFormat,
		OutputFormat: TimeOutputFormat(),
		Granularity:  granularity,
	}
	return timeGroup
}
