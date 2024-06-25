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
`))

type TimeSeriesDriver struct {
	TimeSeriesDriverParams
}

type TimeSeriesDriverParams struct {
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

var AggregationFunctions = []string{"sum", "count", "avg", "max"}

func IsValidAggregationFunction(aggregationFunction string) bool {
	for i := range AggregationFunctions {
		if AggregationFunctions[i] == aggregationFunction {
			return true
		}
	}
	return false
}

func NewTimeSeriesDriver(params TimeSeriesDriverParams) (*TimeSeriesDriver, error) {
	if params.TimeColumn == "" {
		return nil, errors.New("time column cannot be empty")
	} else if params.MetricColumn == "" {
		return nil, errors.New("metric column cannot be empty")
	} else if !IsValidAggregationFunction(params.AggregationFunction) {
		return nil, fmt.Errorf("`%s` is not a valid aggregation function", params.AggregationFunction)
	}
	return &TimeSeriesDriver{params}, nil
}

func (p TimeSeriesDriver) RenderPinotSql() (string, error) {
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

func (p TimeSeriesDriver) getTemplateArgs() (TimeSeriesTemplateArgs, error) {
	exprBuilder, err := TimeExpressionBuilderFor(p.TableSchema, p.TimeColumn)
	if err != nil {
		return TimeSeriesTemplateArgs{}, err
	}

	return TimeSeriesTemplateArgs{
		TableName:           p.TableName,
		TimeColumn:          p.TimeColumn,
		MetricColumn:        p.MetricColumn,
		TimeFilterExpr:      exprBuilder.BuildTimeFilterExpr(p.TimeRange),
		TimeGroupExpr:       exprBuilder.BuildTimeGroupExpr(p.IntervalSize),
		AggregationFunction: p.AggregationFunction,
		TimeColumnAlias:     TimeSeriesTimeColumnAlias,
		MetricColumnAlias:   TimeSeriesMetricColumnAlias,
	}, nil
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

	metColIdx, ok := GetColumnIdx(results, TimeSeriesMetricColumnAlias)
	if !ok {
		return nil, fmt.Errorf("metric column not found")
	}
	metCol := ExtractTypedColumn[float64](results, metColIdx, results.GetDouble)

	if len(p.DimensionColumns) == 0 {
		frame.Fields = append(frame.Fields, data.NewField(p.MetricColumn, nil, metCol))
		return frame, nil
	}
	return nil, nil
}

type Metric struct {
	name      string
	timestamp time.Time
	value     float64
}

func (p TimeSeriesDriver) do(results *pinot.ResultTable) (*data.Frame, error) {
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
		dimensions[results.GetColumnName(colIdx)] = ExtractStringColumn(results, colIdx)
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

	distinctTimeCol := GetDistinctValues(timeCol)
	sort.Slice(distinctTimeCol, func(i, j int) bool {
		return distinctTimeCol[i].Before(distinctTimeCol[j])
	})

	timeIndex := make(map[time.Time]int, len(distinctTimeCol))
	for i, val := range distinctTimeCol {
		timeIndex[val] = i
	}

	timeSeries := make(map[string][]*float64)
	for _, met := range metrics {
		name := p.nameFromLabels(met.labels)
		if _, ok := timeSeries[name]; !ok {
			timeSeries[name] = make([]*float64, len(distinctTimeCol))
		}
		colIdx := timeIndex[met.timestamp]
		timeSeries[name][colIdx] = &met.value
	}

	fields := make([]*data.Field, 0, len(timeSeries)+1)
	for tsName, tsCol := range timeSeries {
		fields = append(fields, data.NewField(tsName, nil, tsCol))
	}
	return data.NewFrame("response", fields...), nil
}

func (p TimeSeriesDriver) nameFromLabels(labels map[string]string) string {
	formatted := make([]string, 0, len(labels)+1)
	formatted = append(formatted, fmt.Sprintf("__name__=%s", p.MetricColumn))
	for key, value := range labels {
		formatted = append(formatted, fmt.Sprintf("%s=%s", key, value))
	}
	sort.Strings(formatted[1:])
	return fmt.Sprintf("%s{%s}", p.MetricColumn, strings.Join(formatted, ","))
}
