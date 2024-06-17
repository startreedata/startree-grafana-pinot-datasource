package plugin

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/pinot-client-go/pinot"
	"strings"
	"text/template"
)

type TimeSeriesContext struct {
	TableName           string
	TimeColumn          string
	MetricColumn        string
	DimensionColumns    []DimensionData
	AggregationFunction string
	IncludeOverall      bool
}

const TimeSeriesTimeColumnAlias = "ts"
const TimeSeriesMetricColumnAlias = "met"

var timeSeriesSqlTemplate = template.Must(template.New("pinot-time-series-template").Parse(`
SELECT
	{{ range .PivotColumnExprs }} {{ . }}, {{ end }}
	{{.TimeGroupExpr}} AS {{.TimeColumnAlias}},
	{{.AggregationFunction}}("{{.MetricColumn}}") AS {{.MetricColumnAlias}}
FROM
    "{{.TableName}}"
WHERE
    {{.TimeFilterExpr}}
GROUP BY
    {{.TimeGroupExpr}}
LIMIT {{.Limit}}
`))

type DimensionData struct {
	Name       string
	ValueExprs []string // This is the value as a sql expression. Underlying strings should be quoted.
}

type PivotColumn struct {
	Alias    string
	WhenExpr string
}

func constructPivotColumnExprs(metricColumn string, aggregationFunction string, dimensions []DimensionData) []string {
	pivotColumns := constructPivotColumns(metricColumn, dimensions)
	pivotColumnExprs := make([]string, len(pivotColumns))
	for i := range pivotColumns {
		pivotColumnExprs[i] = pivotColumnExpr(metricColumn, aggregationFunction, pivotColumns[i])
	}
	return pivotColumnExprs
}

func pivotColumnExpr(metricColumn string, aggregationFunction string, pivotColumn PivotColumn) string {
	switch aggregationFunction {
	case "sum":
		return fmt.Sprintf(`SUM(CASE %s THEN "%s" ELSE 0) as %s`, pivotColumn.WhenExpr, metricColumn, pivotColumn.Alias)
	default: // Count
		return fmt.Sprintf(`SUM(CASE %s THEN 1 ELSE 0) as %s`, pivotColumn.WhenExpr, pivotColumn.Alias)
	}
}

func constructPivotColumns(metricColumn string, dimensions []DimensionData) []PivotColumn {
	dimensionColumns := make([]string, len(dimensions))
	for i := range dimensions {
		dimensionColumns[i] = dimensions[i].Name
	}

	distinctValues := make(map[string][]string, len(dimensions))
	for _, d := range dimensions {
		distinctValues[d.Name] = d.ValueExprs
	}

	valueCombinations := cartesianProduct(distinctValues, dimensionColumns)
	pivotColumns := make([]PivotColumn, 0, len(valueCombinations))
	for _, values := range valueCombinations {
		pivotColumns = append(pivotColumns, PivotColumn{
			Alias:    getPivotColumnAlias(metricColumn, dimensionColumns, values),
			WhenExpr: getPivotColumnWhenExpr(dimensionColumns, values),
		})
	}
	return pivotColumns
}

func getPivotColumnAlias(metricColumn string, dimensions []string, values []string) string {
	names := make([]string, len(dimensions)+1)
	names[0] = fmt.Sprintf("__name__=%s", metricColumn)
	for i := range dimensions {
		names[i+1] = fmt.Sprintf("%s=%s", dimensions[i], strings.Trim(values[i], `'`))
	}
	return fmt.Sprintf("{%s}", strings.Join(names, ","))
}

func getPivotColumnWhenExpr(dimensions []string, values []string) string {
	conditions := make([]string, len(dimensions))
	for i := range dimensions {
		conditions[i] = fmt.Sprintf("%s = %s", dimensions[i], values[i])
	}
	return strings.Join(conditions, " AND ")
}

// Function to generate all combinations of distinct values
// TODO: Can I optimize this at all? Does it matter?
func cartesianProduct(distinctValues map[string][]string, dimensionColumns []string) [][]string {
	var result [][]string

	if len(dimensionColumns) == 0 {
		return [][]string{{}}
	}

	firstDim := dimensionColumns[0]
	restDims := dimensionColumns[1:]

	for _, val := range distinctValues[firstDim] {
		for _, combination := range cartesianProduct(distinctValues, restDims) {
			result = append(result, append([]string{val}, combination...))
		}
	}
	return result
}

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
	queryCtx    QueryContext
	pinotClient PinotClient
	timeFormat  string
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
