package dataquery

import (
	"context"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/templates"
	"strings"
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
	MetricColumn        string
	GroupByColumns      []string
	AggregationFunction string
	DimensionFilters    []DimensionFilter
	Limit               int64
	Granularity         string
	MaxDataPoints       int64
	OrderByClauses      []OrderByClause
	QueryOptions        []QueryOption
	Legend              string
}

func ValidateBuilderParams(params PinotQlBuilderParams) error {
	switch {
	case params.TableName == "":
		return errors.New("TableName is required")
	case params.TimeColumn == "":
		return errors.New("TimeColumn is required")
	case params.MetricColumn == "" && params.AggregationFunction != AggregationFunctionCount:
		return errors.New("MetricColumn is required")
	case params.AggregationFunction == "":
		return errors.New("AggregationFunction is required")
	default:
		return nil
	}
}

func NewPinotQlBuilderDriver(client *pinotlib.PinotClient, params PinotQlBuilderParams) (DriverFunc, error) {
	return func(ctx context.Context) backend.DataResponse {
		return ExecutePinotQlBuilderQuery(ctx, client, params)
	}, nil
}

func ExecutePinotQlBuilderQuery(ctx context.Context, client *pinotlib.PinotClient, params PinotQlBuilderParams) backend.DataResponse {
	if err := ValidateBuilderParams(params); err != nil {
		return NewPluginErrorResponse(err)
	}

	tableConfigs, err := client.ListTableConfigs(ctx, params.TableName)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	granularity := ResolveGranularity(ctx, params.Granularity, params.IntervalSize)
	timeGroup, err := TimeGroupOf(ctx, client, params.TableName, params.TimeColumn, granularity)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	sql, err := renderPinotQlBuilderSql(params, timeGroup, tableConfigs, granularity)
	if err != nil {
		return NewPluginErrorResponse(err)
	}
	results, exceptions, ok, backendResp := doSqlQuery(ctx, client, pinotlib.NewSqlQuery(sql))
	if !ok {
		return backendResp
	}

	frame, err := ExtractTimeSeriesDataFrame(TimeSeriesExtractorParams{
		MetricName:        resolveMetricName(params.AggregationFunction, params.MetricColumn),
		Legend:            params.Legend,
		TimeColumnAlias:   DefaultTimeColumnAlias,
		MetricColumnAlias: DefaultMetricColumnAlias,
		TimeColumnFormat:  resolveTimeColumnFormat(params.AggregationFunction, timeGroup),
	}, results)

	if err != nil {
		return NewPluginErrorResponse(err)
	}
	return NewSqlQueryDataResponse(frame, exceptions)
}

func RenderPinotQlBuilderSql(ctx context.Context, client *pinotlib.PinotClient, params PinotQlBuilderParams) (string, error) {
	if err := ValidateBuilderParams(params); err != nil {
		return "", err
	}

	tableConfigs, err := client.ListTableConfigs(ctx, params.TableName)
	if err != nil {
		return "", err
	}

	granularity := ResolveGranularity(ctx, params.Granularity, params.IntervalSize)
	timeGroup, err := TimeGroupOf(ctx, client, params.TableName, params.TimeColumn, granularity)
	if err != nil {
		return "", err
	}

	return renderPinotQlBuilderSql(params, timeGroup, tableConfigs, granularity)
}

func renderPinotQlBuilderSql(params PinotQlBuilderParams, timeGroup pinotlib.DateTimeConversion, tableConfigs pinotlib.ListTableConfigsResponse, granularity pinotlib.Granularity) (string, error) {
	if params.AggregationFunction == AggregationFunctionNone {
		return templates.RenderSingleMetricSql(templates.SingleMetricSqlParams{
			TableNameExpr:         pinotlib.ObjectExpr(params.TableName),
			TimeColumn:            params.TimeColumn,
			TimeColumnAliasExpr:   pinotlib.ObjectExpr(DefaultTimeColumnAlias),
			MetricColumn:          params.MetricColumn,
			MetricColumnAliasExpr: pinotlib.ObjectExpr(DefaultMetricColumnAlias),
			TimeFilterExpr: pinotlib.TimeFilterExpr(pinotlib.TimeFilter{
				Column: params.TimeColumn,
				Format: timeGroup.InputFormat,
				From:   params.TimeRange.From,
				To:     params.TimeRange.To,
			}),
			DimensionFilterExprs: FilterExprsFrom(params.DimensionFilters),
			Limit:                resolveLimit(params),
			QueryOptionsExpr:     queryOptionsExpr(params.QueryOptions),
		})
	} else {
		return templates.RenderTimeSeriesSql(templates.TimeSeriesSqlParams{
			TableNameExpr:         pinotlib.ObjectExpr(params.TableName),
			TimeGroupExpr:         pinotlib.TimeGroupExpr(tableConfigs, timeGroup),
			TimeColumnAliasExpr:   pinotlib.ObjectExpr(DefaultTimeColumnAlias),
			AggregationFunction:   params.AggregationFunction,
			MetricColumn:          resolveMetricColumn(params.AggregationFunction, params.MetricColumn),
			MetricColumnAliasExpr: pinotlib.ObjectExpr(DefaultMetricColumnAlias),
			GroupByColumns:        params.GroupByColumns,
			TimeFilterExpr: pinotlib.TimeFilterBucketAlignedExpr(pinotlib.TimeFilter{
				Column: params.TimeColumn,
				Format: timeGroup.InputFormat,
				From:   params.TimeRange.From,
				To:     params.TimeRange.To,
			}, granularity.Duration()),
			DimensionFilterExprs: FilterExprsFrom(params.DimensionFilters),
			Limit:                resolveLimit(params),
			OrderByExprs:         orderByExprs(params.OrderByClauses),
			QueryOptionsExpr:     queryOptionsExpr(params.QueryOptions),
		})
	}
}

func RenderPinotQlBuilderSqlWithMacros(ctx context.Context, params PinotQlBuilderParams) (string, error) {
	if err := ValidateBuilderParams(params); err != nil {
		return "", err
	}

	granularity := ResolveGranularity(ctx, params.Granularity, params.IntervalSize)
	if params.AggregationFunction == AggregationFunctionNone {
		return templates.RenderSingleMetricSql(templates.SingleMetricSqlParams{
			TableNameExpr:         MacroExprFor(MacroTable),
			TimeColumn:            params.TimeColumn,
			TimeColumnAliasExpr:   MacroExprFor(MacroTimeAlias),
			MetricColumn:          params.MetricColumn,
			MetricColumnAliasExpr: MacroExprFor(MacroMetricAlias),
			TimeFilterExpr:        MacroExprFor(MacroTimeFilter, pinotlib.ObjectExpr(params.TimeColumn), pinotlib.GranularityExpr(granularity)),
			DimensionFilterExprs:  FilterExprsFrom(params.DimensionFilters),
			Limit:                 resolveLimit(params),
			QueryOptionsExpr:      queryOptionsExpr(params.QueryOptions),
		})
	} else {
		return templates.RenderTimeSeriesSql(templates.TimeSeriesSqlParams{
			TableNameExpr:         MacroExprFor(MacroTable),
			TimeGroupExpr:         MacroExprFor(MacroTimeGroup, pinotlib.ObjectExpr(params.TimeColumn), pinotlib.GranularityExpr(granularity)),
			TimeColumnAliasExpr:   MacroExprFor(MacroTimeAlias),
			AggregationFunction:   params.AggregationFunction,
			MetricColumn:          resolveMetricColumn(params.AggregationFunction, params.MetricColumn),
			MetricColumnAliasExpr: MacroExprFor(MacroMetricAlias),
			GroupByColumns:        params.GroupByColumns,
			TimeFilterExpr:        MacroExprFor(MacroTimeFilter, pinotlib.ObjectExpr(params.TimeColumn), pinotlib.GranularityExpr(granularity)),
			DimensionFilterExprs:  FilterExprsFrom(params.DimensionFilters),
			Limit:                 resolveLimit(params),
			OrderByExprs:          orderByExprs(params.OrderByClauses),
			QueryOptionsExpr:      queryOptionsExpr(params.QueryOptions),
		})
	}
}

func resolveTimeColumnFormat(aggregationFunction string, timeGroup pinotlib.DateTimeConversion) pinotlib.DateTimeFormat {
	if aggregationFunction == AggregationFunctionNone {
		return timeGroup.InputFormat
	} else {
		return timeGroup.OutputFormat
	}
}

func resolveMetricName(aggregationFunction string, metricColumn string) string {
	if aggregationFunction == AggregationFunctionCount {
		return "count"
	} else {
		return metricColumn
	}
}

func resolveMetricColumn(aggregationFunction string, metricColumn string) string {
	if aggregationFunction == AggregationFunctionCount {
		return "*"
	} else {
		return metricColumn
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

func orderByExprs(clauses []OrderByClause) []string {
	exprs := make([]string, 0, len(clauses))
	for _, o := range clauses {
		if o.ColumnName == "" {
			continue
		}

		var direction string
		if strings.ToUpper(o.Direction) == "DESC" {
			direction = "DESC"
		} else {
			direction = "ASC"
		}

		exprs = append(exprs, fmt.Sprintf(`"%s" %s`, o.ColumnName, direction))
	}
	return exprs[:]
}

func queryOptionsExpr(queryOptions []QueryOption) string {
	exprs := make([]string, 0, len(queryOptions))
	for _, o := range queryOptions {
		if o.Name != "" && o.Value != "" {
			exprs = append(exprs, fmt.Sprintf(`SET %s=%s;`, o.Name, o.Value))
		}
	}
	return strings.Join(exprs, "\n")
}
