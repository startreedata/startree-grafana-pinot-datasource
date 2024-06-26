package plugin

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/pinot-client-go/pinot"
	"sort"
	"strings"
	"text/template"
	"time"
)

const TimeSeriesTimeColumnAlias = "ts"
const TimeSeriesMetricColumnAlias = "met"

var timeSeriesSqlTemplate = template.Must(template.New("pinot/time-series-sql").Parse(`
SELECT {{ range .DimensionColumns }} 
    "{{ . }}", 
    {{- end }}
    {{.TimeGroupExpr}} AS "{{.TimeColumnAlias}}",
    {{.AggregationFunction}}("{{.MetricColumn}}") AS "{{.MetricColumnAlias}}"
FROM
    "{{.TableName}}"
WHERE
    {{.TimeFilterExpr}}
GROUP BY {{ range .DimensionColumns }} 
    "{{ . }}", 
    {{- end }}
    {{.TimeGroupExpr}}
ORDER BY "{{.MetricColumnAlias}}" DESC
LIMIT 1000000
`))

type PinotQlBuilderDriver struct {
	PinotQlBuilderParams
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
}

type TimeSeriesMetric struct {
	timestamp time.Time
	value     float64
	labels    map[string]string
}

type TimeSeriesTemplateArgs struct {
	TableName           string
	DimensionColumns    []string
	TimeColumn          string
	MetricColumn        string
	AggregationFunction string
	TimeFilterExpr      string
	TimeGroupExpr       string
	TimeColumnAlias     string
	MetricColumnAlias   string
}

func IsValidAggregationFunction(aggregationFunction string) bool {
	var aggregationFunctions = []string{"SUM", "COUNT", "AVG", "MAX"}
	for i := range aggregationFunctions {
		if aggregationFunctions[i] == strings.ToUpper(aggregationFunction) {
			return true
		}
	}
	return false
}

func NewPinotQlBuilderDriver(params PinotQlBuilderParams) (*PinotQlBuilderDriver, error) {
	if params.TimeColumn == "" {
		return nil, errors.New("time column cannot be empty")
	} else if params.MetricColumn == "" {
		return nil, errors.New("metric column cannot be empty")
	} else if !IsValidAggregationFunction(params.AggregationFunction) {
		return nil, fmt.Errorf("`%s` is not a valid aggregation function", params.AggregationFunction)
	}
	return &PinotQlBuilderDriver{params}, nil
}

func (p PinotQlBuilderDriver) RenderPinotSql() (string, error) {
	Logger.Info("time-series-driver: rendering time series pinot query")

	templArgs, err := p.getTemplateArgs()
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	if err = timeSeriesSqlTemplate.Execute(&buf, templArgs); err != nil {
		return "", fmt.Errorf("failed execute template: %w", err)
	}

	return buf.String(), nil
}

func (p PinotQlBuilderDriver) getTemplateArgs() (TimeSeriesTemplateArgs, error) {
	exprBuilder, err := TimeExpressionBuilderFor(p.TableSchema, p.TimeColumn)
	if err != nil {
		return TimeSeriesTemplateArgs{}, err
	}

	return TimeSeriesTemplateArgs{
		TableName:           p.TableName,
		TimeColumn:          p.TimeColumn,
		MetricColumn:        p.MetricColumn,
		DimensionColumns:    p.DimensionColumns,
		TimeFilterExpr:      exprBuilder.BuildTimeFilterExpr(p.TimeRange),
		TimeGroupExpr:       exprBuilder.BuildTimeGroupExpr(p.IntervalSize),
		AggregationFunction: p.AggregationFunction,
		TimeColumnAlias:     TimeSeriesTimeColumnAlias,
		MetricColumnAlias:   TimeSeriesMetricColumnAlias,
	}, nil
}

func (p PinotQlBuilderDriver) ExtractResults(results *pinot.ResultTable) (*data.Frame, error) {
	metrics, err := p.extractTimeSeriesMetrics(results)
	if err != nil {
		return nil, err
	}
	return p.pivotMetrics(metrics), nil
}

func (p PinotQlBuilderDriver) extractTimeSeriesMetrics(results *pinot.ResultTable) ([]TimeSeriesMetric, error) {
	timeColIdx, ok := GetColumnIdx(results, TimeSeriesTimeColumnAlias)
	if !ok {
		return nil, fmt.Errorf("time column not found")
	}

	timeCol, err := ExtractTimeColumn(results, timeColIdx, TimeGroupExprOutputFormat)
	if err != nil {
		return nil, err
	}

	metColIdx, ok := GetColumnIdx(results, TimeSeriesMetricColumnAlias)
	if !ok {
		return nil, fmt.Errorf("metric column not found")
	}
	metCol := ExtractTypedColumn[float64](results, metColIdx, results.GetDouble)

	dimensions := make(map[string][]string)
	for colIdx := 0; colIdx < results.GetColumnCount(); colIdx++ {
		if colIdx == timeColIdx || colIdx == metColIdx {
			continue
		}
		name := results.GetColumnName(colIdx)
		dimensions[name] = ExtractStringColumn(results, colIdx)
	}

	metrics := make([]TimeSeriesMetric, results.GetRowCount())
	for rowIdx := 0; rowIdx < results.GetRowCount(); rowIdx++ {
		labels := make(map[string]string, len(dimensions))
		for name, col := range dimensions {
			labels[name] = col[rowIdx]
		}

		metrics[rowIdx] = TimeSeriesMetric{
			timestamp: timeCol[rowIdx],
			value:     metCol[rowIdx],
			labels:    labels,
		}
	}

	return metrics, nil
}

func (p PinotQlBuilderDriver) pivotMetrics(metrics []TimeSeriesMetric) *data.Frame {
	timeCol := p.getTimeColum(metrics)

	timeIndex := make(map[time.Time]int, len(timeCol))
	for i, val := range timeCol {
		timeIndex[val] = i
	}

	timeSeries := make(map[string][]*float64)
	for _, met := range metrics {
		name := p.metricNameFromLabels(met.labels)
		if _, ok := timeSeries[name]; !ok {
			timeSeries[name] = make([]*float64, len(timeCol))
		}
		colIdx := timeIndex[met.timestamp]
		value := met.value
		timeSeries[name][colIdx] = &value
	}

	fields := make([]*data.Field, 0, len(timeSeries)+1)
	for tsName, tsCol := range timeSeries {
		fields = append(fields, data.NewField(tsName, nil, tsCol))
	}
	fields = append(fields, data.NewField("time", nil, timeCol))
	return data.NewFrame("response", fields...)
}

func (p PinotQlBuilderDriver) getTimeColum(metrics []TimeSeriesMetric) []time.Time {
	observed := make(map[time.Time]interface{})
	result := make([]time.Time, 0, len(metrics))
	for _, metric := range metrics {
		val := metric.timestamp
		if _, ok := observed[val]; !ok {
			result = append(result, val)
			observed[val] = nil
		}
	}
	return result[:]
}

func (p PinotQlBuilderDriver) metricNameFromLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return p.MetricColumn
	}

	formatted := make([]string, 0, len(labels))
	for key, value := range labels {
		formatted = append(formatted, fmt.Sprintf("%s=%s", key, value))
	}
	sort.Strings(formatted)
	return fmt.Sprintf(`%s{%s}`, p.MetricColumn, strings.Join(formatted, ","))
}
