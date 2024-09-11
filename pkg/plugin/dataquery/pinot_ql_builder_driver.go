package dataquery

import (
	"context"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startree/pinot/pkg/plugin/pinotlib"
	"github.com/startree/pinot/pkg/plugin/templates"
	"github.com/startreedata/pinot-client-go/pinot"
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
	PinotQlBuilderParams
	TimeExpressionBuilder
	TimeColumnAlias   string
	MetricColumnAlias string
	TimeGranularity   TimeGranularity
}

type PinotQlBuilderParams struct {
	*pinotlib.PinotClient
	pinotlib.TableSchema
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

	exprBuilder, err := TimeExpressionBuilderFor(params.TableSchema, params.TimeColumn)
	if err != nil {
		return nil, err
	}

	timeGranularity, err := TimeGranularityFrom(params.Granularity, params.IntervalSize)
	if err != nil {
		return nil, err
	}

	return &PinotQlBuilderDriver{
		PinotQlBuilderParams:  params,
		TimeColumnAlias:       DefaultTimeColumnAlias,
		MetricColumnAlias:     DefaultMetricColumnAlias,
		TimeExpressionBuilder: exprBuilder,
		TimeGranularity:       timeGranularity,
	}, nil
}

func (p *PinotQlBuilderDriver) Execute(ctx context.Context) backend.DataResponse {
	sql, err := p.RenderPinotSql()
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}

	resp, err := p.ExecuteSQL(ctx, p.TableName, sql)
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}

	frame, err := p.ExtractResults(resp.ResultTable)
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}
	return NewDataResponse(frame)
}

func (p *PinotQlBuilderDriver) RenderPinotSqlWithMacros() (string, error) {
	if p.AggregationFunction == AggregationFunctionNone {
		return templates.RenderSingleMetricSql(templates.SingleMetricSqlParams{
			TableNameExpr:         MacroExprFor(MacroTable),
			TimeColumn:            p.TimeColumn,
			TimeColumnAliasExpr:   MacroExprFor(MacroTimeAlias),
			MetricColumn:          p.MetricColumn,
			MetricColumnAliasExpr: MacroExprFor(MacroMetricAlias),
			TimeFilterExpr:        MacroExprFor(MacroTimeFilter, fmt.Sprintf(`"%s"`, p.TimeColumn)),
			DimensionFilterExprs:  FilterExprsFrom(p.DimensionFilters),
			Limit:                 p.resolveLimit(),
			QueryOptionsExpr:      p.queryOptionsExpr(),
		})
	} else {
		return templates.RenderTimeSeriesSql(templates.TimeSeriesSqlParams{
			TableNameExpr:         MacroExprFor(MacroTable),
			TimeGroupExpr:         MacroExprFor(MacroTimeGroup, SqlObjectExpr(p.TimeColumn)),
			TimeColumnAliasExpr:   MacroExprFor(MacroTimeAlias),
			AggregationFunction:   p.AggregationFunction,
			MetricColumn:          p.resolveMetricColumn(),
			MetricColumnAliasExpr: MacroExprFor(MacroMetricAlias),
			GroupByColumns:        p.GroupByColumns,
			TimeFilterExpr:        MacroExprFor(MacroTimeFilter, SqlObjectExpr(p.TimeColumn)),
			DimensionFilterExprs:  FilterExprsFrom(p.DimensionFilters),
			Limit:                 p.resolveLimit(),
			OrderByExprs:          p.orderByExprs(),
			QueryOptionsExpr:      p.queryOptionsExpr(),
		})
	}
}

func (p *PinotQlBuilderDriver) RenderPinotSql() (string, error) {
	if p.AggregationFunction == AggregationFunctionNone {
		return templates.RenderSingleMetricSql(templates.SingleMetricSqlParams{
			TableNameExpr:         SqlObjectExpr(p.TableName),
			TimeColumn:            p.TimeColumn,
			TimeColumnAliasExpr:   SqlObjectExpr(p.TimeColumnAlias),
			MetricColumn:          p.MetricColumn,
			MetricColumnAliasExpr: SqlObjectExpr(p.MetricColumnAlias),
			TimeFilterExpr:        p.TimeFilterExpr(p.TimeRange),
			DimensionFilterExprs:  FilterExprsFrom(p.DimensionFilters),
			Limit:                 p.resolveLimit(),
			QueryOptionsExpr:      p.queryOptionsExpr(),
		})
	} else {
		return templates.RenderTimeSeriesSql(templates.TimeSeriesSqlParams{
			TableNameExpr:         SqlObjectExpr(p.TableName),
			TimeGroupExpr:         p.TimeGroupExpr(p.TimeGranularity.Expr),
			TimeColumnAliasExpr:   SqlObjectExpr(p.TimeColumnAlias),
			AggregationFunction:   p.AggregationFunction,
			MetricColumn:          p.resolveMetricColumn(),
			MetricColumnAliasExpr: SqlObjectExpr(p.MetricColumnAlias),
			GroupByColumns:        p.GroupByColumns,
			TimeFilterExpr:        p.TimeFilterBucketAlignedExpr(p.TimeRange, p.TimeGranularity.Size),
			DimensionFilterExprs:  FilterExprsFrom(p.DimensionFilters),
			Limit:                 p.resolveLimit(),
			OrderByExprs:          p.orderByExprs(),
			QueryOptionsExpr:      p.queryOptionsExpr(),
		})
	}
}

func (p *PinotQlBuilderDriver) ExtractResults(results *pinot.ResultTable) (*data.Frame, error) {
	return ExtractTimeSeriesDataFrame(TimeSeriesExtractorParams{
		MetricName:        p.resolveMetricName(),
		Legend:            p.Legend,
		TimeColumnAlias:   p.TimeColumnAlias,
		TimeColumnFormat:  p.resolveTimeColumnFormat(),
		MetricColumnAlias: p.MetricColumnAlias,
	}, results)
}

func (p *PinotQlBuilderDriver) resolveTimeColumnFormat() string {
	if p.AggregationFunction == AggregationFunctionNone {
		return p.TimeExpressionBuilder.TimeColumnFormat()
	} else {
		return TimeGroupExprOutputFormat
	}
}

func (p *PinotQlBuilderDriver) resolveMetricName() string {
	if p.AggregationFunction == AggregationFunctionCount {
		return "count"
	} else {
		return p.MetricColumn
	}
}

func (p *PinotQlBuilderDriver) resolveLimit() int64 {
	switch true {
	case p.Limit >= 1:
		// Use provided limit if present
		return p.Limit
	case p.AggregationFunction != AggregationFunctionNone && len(p.GroupByColumns) > 0:
		// Use default limit for group by queries.
		// TODO: Resolve more accurate limit in this case.
		return DefaultLimit
	case p.MaxDataPoints > 0:
		// Queries with extra dimensions can directly use max data points.
		return p.MaxDataPoints
	default:
		return DefaultLimit
	}
}

func (p *PinotQlBuilderDriver) resolveMetricColumn() string {
	if p.AggregationFunction == AggregationFunctionCount {
		return "*"
	} else {
		return p.MetricColumn
	}
}

func (p *PinotQlBuilderDriver) orderByExprs() []string {
	orderByExprs := make([]string, 0, len(p.OrderByClauses))
	for _, o := range p.OrderByClauses {
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
	exprs := make([]string, 0, len(p.QueryOptions))
	for _, o := range p.QueryOptions {
		if o.Name != "" && o.Value != "" {
			exprs = append(exprs, fmt.Sprintf(`SET %s=%s;`, o.Name, o.Value))
		}
	}
	return strings.Join(exprs, "\n")
}
