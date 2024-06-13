package plugin

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/pinot-client-go/pinot"
	"text/template"
)

type TimeSeriesContext struct {
	TableName           string
	TimeColumn          string
	MetricColumn        string
	DimensionColumns    []string
	AggregationFunction string
}

const TimeSeriesTimeColumnAlias = "ts"
const TimeSeriesMetricColumnAlias = "met"

var timeSeriesSqlTemplate = template.Must(template.New("pinot-time-series-template").Parse(`
SELECT
	{{ range .DimensionColumns }} {{ . }}, {{ end }}
	{{.TimeGroupExpr}} AS {{.TimeColumnAlias}}, 
	{{.AggregationFunction}}("{{.MetricColumn}}") AS {{.MetricColumnAlias}}
FROM
    "{{.TableName}}"
WHERE
    {{.TimeFilterExpr}}
GROUP BY
	{{ range .DimensionColumns }} {{ . }}, {{ end }}
    {{.TimeGroupExpr}}
LIMIT {{.Limit}}
`))

type timeSeriesTemplateArgs struct {
	TableName           string
	DimensionColumns    []string
	TimeColumn          string
	MetricColumn        string
	AggregationFunction string
	TimeFilterExpr      string
	TimeGroupExpr       string
	TimeColumnAlias     string
	MetricColumnAlias   string
	Limit               int64
}

type TimeSeriesDriver struct {
	queryCtx   QueryContext
	timeFormat string
}

var AggregationFunctions = []string{"sum", "count", "avg", "max"}

func IsValidAggregationFunction(aggregationFunction string) bool {
	for i := range AggregationFunctions {
		if AggregationFunctions[i] == aggregationFunction {
			return true
		}
	}
	return false
}

func NewTimeSeriesDriver(queryCtx QueryContext) (*TimeSeriesDriver, error) {
	if queryCtx.TimeSeriesContext.TimeColumn == "" {
		return nil, errors.New("time column cannot be empty")
	} else if queryCtx.TimeSeriesContext.MetricColumn == "" {
		return nil, errors.New("metric column cannot be empty")
	} else if !IsValidAggregationFunction(queryCtx.TimeSeriesContext.AggregationFunction) {
		return nil, fmt.Errorf("`%s` is not a valid aggregation function", queryCtx.TimeSeriesContext.AggregationFunction)
	}
	return &TimeSeriesDriver{queryCtx: queryCtx}, nil
}

func (p TimeSeriesDriver) RenderPinotSql() (string, error) {
	Logger.Info("time-series-driver: rendering time series pinot query")
	exprBuilder, err := TimeExpressionBuilderFor(p.queryCtx, p.queryCtx.TimeSeriesContext.TimeColumn)
	if err != nil {
		return "", err
	}

	templArgs := templArgsFor(p.queryCtx, exprBuilder)
	var buf bytes.Buffer
	if err = timeSeriesSqlTemplate.Execute(&buf, templArgs); err != nil {
		return "", fmt.Errorf("failed execute template: %w", err)
	}

	return buf.String(), nil
}

func templArgsFor(queryCtx QueryContext, exprBuilder TimeExpressionBuilder) timeSeriesTemplateArgs {
	return timeSeriesTemplateArgs{
		TableName:           queryCtx.TimeSeriesContext.TableName,
		TimeColumn:          queryCtx.TimeSeriesContext.TimeColumn,
		MetricColumn:        queryCtx.TimeSeriesContext.MetricColumn,
		DimensionColumns:    queryCtx.TimeSeriesContext.DimensionColumns,
		TimeFilterExpr:      exprBuilder.BuildTimeFilterExpr(queryCtx.TimeRange),
		TimeGroupExpr:       exprBuilder.BuildTimeGroupExpr(queryCtx.IntervalSize),
		AggregationFunction: queryCtx.TimeSeriesContext.AggregationFunction,
		TimeColumnAlias:     TimeSeriesTimeColumnAlias,
		MetricColumnAlias:   TimeSeriesMetricColumnAlias,
		Limit:               queryCtx.MaxDataPoints,
	}
}

func (p TimeSeriesDriver) ExtractResults(results *pinot.ResultTable) (*data.Frame, error) {
	frame := data.NewFrame("response")

	timeColIdx, ok := GetColumnIdx(results, TimeSeriesTimeColumnAlias)
	if !ok {
		return nil, fmt.Errorf("time column not found")
	}
	timeCol, err := ExtractTimeColumn(results, timeColIdx, TimeGroupExprOutputFormat)
	if err != nil {
		return nil, err
	}
	frame.Fields = append(frame.Fields, data.NewField("time", nil, timeCol))

	for colIdx := 0; colIdx < results.GetColumnCount(); colIdx++ {
		if colIdx == timeColIdx {
			continue
		}
		frame.Fields = append(frame.Fields, ExtractColumnToField(results, colIdx))
	}
	return frame, nil
}
