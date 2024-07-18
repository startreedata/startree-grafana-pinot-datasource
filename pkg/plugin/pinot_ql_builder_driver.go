package plugin

import (
	"errors"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startree/pinot/pkg/plugin/templates"
	"github.com/startreedata/pinot-client-go/pinot"
	"time"
)

const DefaultTimeColumnAlias = "time"
const DefaultMetricColumnAlias = "metric"

const AggregationFunctionCount = "COUNT"
const AggregationFunctionNone = "NONE"
const GranularityAuto = "auto"
const DefaultLimit = 1_000_000

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
}

func NewPinotQlBuilderDriver(params PinotQlBuilderParams) (Driver, error) {
	if params.TimeColumn == "" {
		return nil, errors.New("time column cannot be empty")
	} else if params.MetricColumn == "" && params.AggregationFunction != AggregationFunctionCount {
		return nil, errors.New("metric column cannot be empty")
	} else if params.AggregationFunction == "" {
		return nil, errors.New("aggregation function cannot be empty")
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
