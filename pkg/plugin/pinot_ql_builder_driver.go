package plugin

import (
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startree/pinot/pkg/plugin/templates"
	"github.com/startreedata/pinot-client-go/pinot"
	"time"
)

const DefaultTimeColumnAlias = "time"
const DefaultMetricColumnAlias = "metric"

type PinotQlBuilderDriver struct {
	PinotQlBuilderParams
	TimeColumnAlias   string
	MetricColumnAlias string
	TimeColumnFormat  string
}

type PinotQlBuilderParams struct {
	TableSchema
	TimeRange           TimeRange
	IntervalSize        time.Duration
	DatabaseName        string
	TableName           string
	TimeColumn          string
	MetricColumn        string
	DimensionColumns    []string
	AggregationFunction string
	DimensionFilters    []DimensionFilter
}

func NewPinotQlBuilderDriver(params PinotQlBuilderParams) (*PinotQlBuilderDriver, error) {
	if params.TimeColumn == "" {
		return nil, errors.New("time column cannot be empty")
	} else if params.MetricColumn == "" {
		return nil, errors.New("metric column cannot be empty")
	} else if !templates.IsValidAggregationFunction(params.AggregationFunction) {
		return nil, fmt.Errorf("`%s` is not a valid aggregation function", params.AggregationFunction)
	}
	return &PinotQlBuilderDriver{
		PinotQlBuilderParams: params,
		TimeColumnAlias:      DefaultTimeColumnAlias,
		TimeColumnFormat:     TimeGroupExprOutputFormat,
		MetricColumnAlias:    DefaultMetricColumnAlias,
	}, nil
}

func (p PinotQlBuilderDriver) RenderPinotSql() (string, error) {
	Logger.Info("time-series-driver: rendering time series pinot query")

	exprBuilder, err := TimeExpressionBuilderFor(p.TableSchema, p.TimeColumn)
	if err != nil {
		return "", err
	}

	return templates.RenderTimeSeriesSql(templates.TimeSeriesSqlParams{
		TableName:           p.TableName,
		TimeColumn:          p.TimeColumn,
		MetricColumn:        p.MetricColumn,
		DimensionColumns:    p.DimensionColumns,
		TimeFilterExpr:      exprBuilder.TimeFilterExpr(p.TimeRange),
		TimeGroupExpr:       exprBuilder.BuildTimeGroupExpr(p.IntervalSize),
		AggregationFunction: p.AggregationFunction,
		TimeColumnAlias:     DefaultTimeColumnAlias,
		MetricColumnAlias:   DefaultMetricColumnAlias,
	})
}

func (p PinotQlBuilderDriver) ExtractResults(results *pinot.ResultTable) (*data.Frame, error) {
	metrics, err := ExtractTimeSeriesMetrics(results, p.TimeColumnAlias, p.TimeColumnFormat, p.MetricColumnAlias)
	if err != nil {
		return nil, err
	}
	return PivotToDataFrame(p.MetricColumn, metrics), nil
}
