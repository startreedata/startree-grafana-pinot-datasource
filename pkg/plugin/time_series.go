package plugin

import (
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/pinot-client-go/pinot"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
)

type TimeSeriesExtractorParams struct {
	MetricName        string
	Legend            string
	TimeColumnAlias   string
	TimeColumnFormat  string
	MetricColumnAlias string
}

type Metric struct {
	Timestamp time.Time
	Value     float64
	Labels    map[string]string
}

type MetricSeries struct {
	name   string
	values []*float64
	labels map[string]string
}

func ExtractTimeSeriesDataFrame(params TimeSeriesExtractorParams, results *pinot.ResultTable) (*data.Frame, error) {
	metrics, err := ExtractMetrics(results, params.TimeColumnAlias, params.TimeColumnFormat, params.MetricColumnAlias)
	if err != nil {
		return nil, err
	}

	timeCol, metricSeries := PivotToTimeSeries(metrics, params.MetricName, params.Legend)
	slices.SortFunc(metricSeries, func(a, b MetricSeries) int {
		return strings.Compare(a.name, b.name)
	})

	fields := make([]*data.Field, 0, len(metricSeries)+1)
	for _, series := range metricSeries {
		fields = append(fields, data.NewField(series.name, series.labels, series.values))
	}
	fields = append(fields, data.NewField("time", nil, timeCol))

	return data.NewFrame("response", fields...), nil
}

func ExtractMetrics(results *pinot.ResultTable, timeColumnAlias string, timeColumnFormat string, metricColumnAlias string) ([]Metric, error) {
	timeColIdx, err := GetColumnIdx(results, timeColumnAlias)
	if err != nil {
		return nil, err
	}

	timeCol, err := ExtractTimeColumn(results, timeColIdx, timeColumnFormat)
	if err != nil {
		return nil, err
	}

	metColIdx, err := GetColumnIdx(results, metricColumnAlias)
	if err != nil {
		return nil, err
	}
	metCol := ExtractDoubleColumn(results, metColIdx)

	dimensions := make(map[string][]string)
	for colIdx := 0; colIdx < results.GetColumnCount(); colIdx++ {
		if colIdx == timeColIdx || colIdx == metColIdx {
			continue
		}
		name := results.GetColumnName(colIdx)
		dimensions[name] = ExtractStringColumn(results, colIdx)
	}

	metrics := make([]Metric, results.GetRowCount())
	for rowIdx := 0; rowIdx < results.GetRowCount(); rowIdx++ {
		labels := make(map[string]string, len(dimensions))
		for name, col := range dimensions {
			labels[name] = col[rowIdx]
		}

		metrics[rowIdx] = Metric{
			Timestamp: timeCol[rowIdx],
			Value:     metCol[rowIdx],
			Labels:    labels,
		}
	}

	return metrics, nil
}

func PivotToTimeSeries(metrics []Metric, metricName string, legend string) ([]time.Time, []MetricSeries) {
	timeCol := GetTimeColumn(metrics)

	timestampToIdx := make(map[time.Time]int, len(timeCol))
	for i, val := range timeCol {
		timestampToIdx[val] = i
	}

	timeSeriesMap := make(map[string]MetricSeries)
	for _, met := range metrics {
		tsKey := GetSeriesKey(metricName, met.Labels)
		if _, ok := timeSeriesMap[tsKey]; !ok {
			timeSeriesMap[tsKey] = MetricSeries{
				name:   FormatSeriesName(tsKey, legend, met.Labels),
				values: make([]*float64, len(timeCol)),
				labels: met.Labels,
			}
		}
		colIdx := timestampToIdx[met.Timestamp]
		value := met.Value
		timeSeriesMap[tsKey].values[colIdx] = &value
	}

	metricSeries := make([]MetricSeries, 0, len(timeSeriesMap))
	for _, ts := range timeSeriesMap {
		metricSeries = append(metricSeries, ts)
	}
	return timeCol, metricSeries
}

func GetTimeColumn(metrics []Metric) []time.Time {
	observed := make(map[time.Time]bool)
	result := make([]time.Time, 0, len(metrics))
	for _, metric := range metrics {
		val := metric.Timestamp
		if !observed[val] {
			result = append(result, val)
			observed[val] = true
		}
	}
	return result[:]
}

func FormatSeriesName(defaultName string, legend string, labels map[string]string) string {
	legend = strings.TrimSpace(legend)
	if legend == "" {
		return defaultName
	} else if !strings.Contains(legend, "{{") {
		return legend
	}

	for key, val := range labels {
		pattern := fmt.Sprintf(`\{\{\s*%s\s*}}`, regexp.QuoteMeta(key))
		r, err := regexp.Compile(pattern)
		if err != nil {
			Logger.Info("Error compiling legend regex", err)
			continue
		}
		legend = r.ReplaceAllString(legend, val)
	}
	return legend
}

// GetSeriesKey returns a unique string for the set of labels.
func GetSeriesKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}

	formattedLabels := make([]string, 0, len(labels))
	for key, value := range labels {
		formattedLabels = append(formattedLabels, fmt.Sprintf("%s=%s", key, value))
	}
	sort.Strings(formattedLabels)
	return fmt.Sprintf(`%s{%s}`, name, strings.Join(formattedLabels, ","))
}
