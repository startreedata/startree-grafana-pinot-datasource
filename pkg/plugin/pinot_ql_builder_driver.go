package plugin

import (
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startree/pinot/pkg/plugin/templates"
	"github.com/startreedata/pinot-client-go/pinot"
	"strings"
	"time"
)

const DefaultTimeColumnAlias = "time"
const DefaultMetricColumnAlias = "metric"

const AggregationFunctionCount = "COUNT"
const AggregationFunctionNone = "NONE"
const GranularityAuto = "auto"
const DefaultLimit = 100_000

type PinotQlBuilderDriver struct {
	PinotQlBuilderParams
	TimeExpressionBuilder
	TimeColumnAlias   string
	MetricColumnAlias string
}

type PinotQlBuilderParams struct {
	TableSchema
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

	return &PinotQlBuilderDriver{
		PinotQlBuilderParams:  params,
		TimeColumnAlias:       DefaultTimeColumnAlias,
		MetricColumnAlias:     DefaultMetricColumnAlias,
		TimeExpressionBuilder: exprBuilder,
	}, nil
}

func (p PinotQlBuilderDriver) RenderPinotSql() (string, error) {
	if p.AggregationFunction == AggregationFunctionNone {
		return templates.RenderSingleMetricSql(templates.SingleMetricSqlParams{
			TableName:            p.TableName,
			TimeColumn:           p.TimeColumn,
			TimeColumnAlias:      p.TimeColumnAlias,
			MetricColumn:         p.MetricColumn,
			MetricColumnAlias:    p.MetricColumnAlias,
			TimeFilterExpr:       p.TimeFilterExpr(p.TimeRange),
			DimensionFilterExprs: FilterExprsFrom(p.DimensionFilters),
			Limit:                p.resolveLimit(),
			OrderByExprs:         p.orderByExprs(),
		})
	} else {
		return templates.RenderTimeSeriesSql(templates.TimeSeriesSqlParams{
			TableName:            p.TableName,
			TimeGroupExpr:        p.BuildTimeGroupExpr(p.resolveGranularity()),
			TimeColumnAlias:      p.TimeColumnAlias,
			AggregationFunction:  p.AggregationFunction,
			MetricColumn:         p.resolveMetricColumn(),
			MetricColumnAlias:    p.MetricColumnAlias,
			GroupByColumns:       p.GroupByColumns,
			TimeFilterExpr:       p.TimeFilterExpr(p.TimeRange),
			DimensionFilterExprs: FilterExprsFrom(p.DimensionFilters),
			Limit:                p.resolveLimit(),
			OrderByExprs:         p.orderByExprs(),
		})
	}
}

func (p PinotQlBuilderDriver) ExtractResults(results *pinot.ResultTable) (*data.Frame, error) {
	metrics, err := ExtractTimeSeriesMetrics(results, p.TimeColumnAlias, p.resolveTimeColumnFormat(), p.MetricColumnAlias)
	if err != nil {
		return nil, err
	}
	return PivotToDataFrame(p.resolveMetricName(), metrics), nil
}

func (p PinotQlBuilderDriver) resolveTimeColumnFormat() string {
	if p.AggregationFunction == AggregationFunctionNone {
		return p.TimeExpressionBuilder.TimeColumnFormat()
	} else {
		return TimeGroupExprOutputFormat
	}
}

func (p PinotQlBuilderDriver) resolveMetricName() string {
	if p.AggregationFunction == AggregationFunctionCount {
		return "count"
	} else {
		return p.MetricColumn
	}
}

func (p PinotQlBuilderDriver) resolveLimit() int64 {
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

func (p PinotQlBuilderDriver) resolveGranularity() string {
	if p.Granularity != "" && p.Granularity != GranularityAuto {
		return p.Granularity
	}
	return p.GranularityExpr(p.IntervalSize)
}

func (p PinotQlBuilderDriver) resolveMetricColumn() string {
	if p.AggregationFunction == AggregationFunctionCount {
		return "*"
	} else {
		return p.MetricColumn
	}
}

func (p PinotQlBuilderDriver) orderByExprs() []string {
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
