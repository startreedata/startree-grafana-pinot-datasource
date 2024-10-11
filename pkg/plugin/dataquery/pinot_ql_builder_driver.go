package dataquery

import (
	"context"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/pinot-client-go/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/templates"
	"strings"
	"time"
)

const (
	DefaultTimeColumnAlias   = "time"
	DefaultMetricColumnAlias = "metric"

	AggregationFunctionCount = "COUNT"
	AggregationFunctionNone  = "NONE"

	DefaultLimit = 100_000
)

type PinotQlBuilderDriver struct {
	params            PinotQlBuilderParams
	timeExprBuilder   pinotlib.TimeExpressionBuilder
	TimeColumnAlias   string
	MetricColumnAlias string
	TimeGranularity   TimeGranularity
}

type PinotQlBuilderParams struct {
	PinotClient         *pinotlib.PinotClient
	TableSchema         pinotlib.TableSchema
	TimeRange           TimeRange
	IntervalSize        time.Duration
	DatabaseName        string
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

func NewPinotQlBuilderDriver(params PinotQlBuilderParams) (*PinotQlBuilderDriver, error) {
	if params.TableName == "" {
		return nil, errors.New("TableName is required")
	} else if params.TimeColumn == "" {
		return nil, errors.New("TimeColumn is required")
	} else if params.MetricColumn == "" && params.AggregationFunction != AggregationFunctionCount {
		return nil, errors.New("MetricColumn is required")
	} else if params.AggregationFunction == "" {
		return nil, errors.New("AggregationFunction is required")
	}

	exprBuilder, err := pinotlib.TimeExpressionBuilderFor(params.TableSchema, params.TimeColumn)
	if err != nil {
		return nil, err
	}

	timeGranularity, err := TimeGranularityFrom(params.Granularity, params.IntervalSize)
	if err != nil {
		return nil, err
	}

	return &PinotQlBuilderDriver{
		params:            params,
		TimeColumnAlias:   DefaultTimeColumnAlias,
		MetricColumnAlias: DefaultMetricColumnAlias,
		timeExprBuilder:   exprBuilder,
		TimeGranularity:   timeGranularity,
	}, nil
}

func (p *PinotQlBuilderDriver) Execute(ctx context.Context) backend.DataResponse {
	sql, err := p.RenderPinotSql(true)
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}

	resp, err := p.params.PinotClient.ExecuteSQL(ctx, p.params.TableName, sql)
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}

	frame, err := p.ExtractResults(resp.ResultTable)
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}
	return NewDataResponse(frame)
}

func (p *PinotQlBuilderDriver) RenderPinotSql(expandMacros bool) (string, error) {
	if p.params.AggregationFunction == AggregationFunctionNone {
		return templates.RenderSingleMetricSql(templates.SingleMetricSqlParams{
			TableNameExpr:         p.tableNameExpr(expandMacros),
			TimeColumn:            p.params.TimeColumn,
			TimeColumnAliasExpr:   p.timeColumnAliasExpr(expandMacros),
			MetricColumn:          p.params.MetricColumn,
			MetricColumnAliasExpr: p.metricColumnAliasExpr(expandMacros),
			TimeFilterExpr:        p.timeFilterExpr(expandMacros),
			DimensionFilterExprs:  FilterExprsFrom(p.params.DimensionFilters),
			Limit:                 p.resolveLimit(),
			QueryOptionsExpr:      p.queryOptionsExpr(),
		})
	} else {
		return templates.RenderTimeSeriesSql(templates.TimeSeriesSqlParams{
			TableNameExpr:         p.tableNameExpr(expandMacros),
			TimeGroupExpr:         p.timeGroupExpr(expandMacros),
			TimeColumnAliasExpr:   p.timeColumnAliasExpr(expandMacros),
			AggregationFunction:   p.params.AggregationFunction,
			MetricColumn:          p.resolveMetricColumn(),
			MetricColumnAliasExpr: p.metricColumnAliasExpr(expandMacros),
			GroupByColumns:        p.params.GroupByColumns,
			TimeFilterExpr:        p.timeFilterExpr(expandMacros),
			DimensionFilterExprs:  FilterExprsFrom(p.params.DimensionFilters),
			Limit:                 p.resolveLimit(),
			OrderByExprs:          p.orderByExprs(),
			QueryOptionsExpr:      p.queryOptionsExpr(),
		})
	}
}

func (p *PinotQlBuilderDriver) ExtractResults(results *pinot.ResultTable) (*data.Frame, error) {
	return ExtractTimeSeriesDataFrame(TimeSeriesExtractorParams{
		MetricName:        p.resolveMetricName(),
		Legend:            p.params.Legend,
		TimeColumnAlias:   p.TimeColumnAlias,
		TimeColumnFormat:  p.resolveTimeColumnFormat(),
		MetricColumnAlias: p.MetricColumnAlias,
	}, results)
}

func (p *PinotQlBuilderDriver) tableNameExpr(expandMacros bool) string {
	if expandMacros {
		return pinotlib.SqlObjectExpr(p.params.TableName)
	} else {
		return MacroExprFor(MacroTable)
	}
}

func (p *PinotQlBuilderDriver) timeColumnAliasExpr(expandMacros bool) string {
	if expandMacros {
		return pinotlib.SqlObjectExpr(p.TimeColumnAlias)
	} else {
		return MacroExprFor(MacroTimeAlias)
	}
}

func (p *PinotQlBuilderDriver) metricColumnAliasExpr(expandMacros bool) string {
	if expandMacros {
		return pinotlib.SqlObjectExpr(p.MetricColumnAlias)
	} else {
		return MacroExprFor(MacroMetricAlias)
	}
}

func (p *PinotQlBuilderDriver) timeFilterExpr(expandMacros bool) string {
	if expandMacros {
		return p.timeExprBuilder.TimeFilterBucketAlignedExpr(p.params.TimeRange.From, p.params.TimeRange.To, p.TimeGranularity.Size)
	} else {
		return MacroExprFor(MacroTimeFilter, pinotlib.SqlObjectExpr(p.params.TimeColumn), pinotlib.SqlLiteralStringExpr(p.TimeGranularity.Expr))
	}
}

func (p *PinotQlBuilderDriver) timeGroupExpr(expandMacros bool) string {
	if expandMacros {
		return p.timeExprBuilder.TimeGroupExpr(p.TimeGranularity.Expr)
	} else {
		return MacroExprFor(MacroTimeGroup, pinotlib.SqlObjectExpr(p.params.TimeColumn), pinotlib.SqlLiteralStringExpr(p.TimeGranularity.Expr))
	}
}

func (p *PinotQlBuilderDriver) resolveTimeColumnFormat() string {
	if p.params.AggregationFunction == AggregationFunctionNone {
		return p.timeExprBuilder.TimeColumnFormat()
	} else {
		return pinotlib.TimeGroupExprOutputFormat
	}
}

func (p *PinotQlBuilderDriver) resolveMetricName() string {
	if p.params.AggregationFunction == AggregationFunctionCount {
		return "count"
	} else {
		return p.params.MetricColumn
	}
}

func (p *PinotQlBuilderDriver) resolveLimit() int64 {
	switch true {
	case p.params.Limit >= 1:
		// Use provided limit if present
		return p.params.Limit
	case p.params.AggregationFunction != AggregationFunctionNone && len(p.params.GroupByColumns) > 0:
		// Use default limit for group by queries.
		// TODO: Resolve more accurate limit in this case.
		return DefaultLimit
	case p.params.MaxDataPoints > 0:
		// Queries with extra dimensions can directly use max data points.
		return p.params.MaxDataPoints
	default:
		return DefaultLimit
	}
}

func (p *PinotQlBuilderDriver) resolveMetricColumn() string {
	if p.params.AggregationFunction == AggregationFunctionCount {
		return "*"
	} else {
		return p.params.MetricColumn
	}
}

func (p *PinotQlBuilderDriver) orderByExprs() []string {
	orderByExprs := make([]string, 0, len(p.params.OrderByClauses))
	for _, o := range p.params.OrderByClauses {
		if o.ColumnName == "" {
			continue
		}

		var direction string
		if strings.ToUpper(o.Direction) == "DESC" {
			direction = "DESC"
		} else {
			direction = "ASC"
		}

		orderByExprs = append(orderByExprs, fmt.Sprintf(`"%s" %s`, o.ColumnName, direction))
	}
	return orderByExprs[:]
}

func (p *PinotQlBuilderDriver) queryOptionsExpr() string {
	exprs := make([]string, 0, len(p.params.QueryOptions))
	for _, o := range p.params.QueryOptions {
		if o.Name != "" && o.Value != "" {
			exprs = append(exprs, fmt.Sprintf(`SET %s=%s;`, o.Name, o.Value))
		}
	}
	return strings.Join(exprs, "\n")
}
