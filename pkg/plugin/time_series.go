package plugin

import (
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/pinot-client-go/pinot"
	"sort"
	"strings"
	"time"
)

type TimeSeriesMetric struct {
	Timestamp time.Time
	Value     float64
	Labels    map[string]string
}

func ExtractTimeSeriesMetrics(results *pinot.ResultTable, timeColumnAlias string, timeColumnFormat string, metricColumnAlias string) ([]TimeSeriesMetric, error) {
	timeColIdx, ok := GetColumnIdx(results, timeColumnAlias)
	if !ok {
		return nil, fmt.Errorf("time column not found")
	}

	timeCol, err := ExtractTimeColumn(results, timeColIdx, timeColumnFormat)
	if err != nil {
		return nil, err
	}

	metColIdx, ok := GetColumnIdx(results, metricColumnAlias)
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
			Timestamp: timeCol[rowIdx],
			Value:     metCol[rowIdx],
			Labels:    labels,
		}
	}

	return metrics, nil
}

func PivotToDataFrame(name string, metrics []TimeSeriesMetric) *data.Frame {
	timeCol := GetTimeColumn(metrics)

	timestampToIdx := make(map[time.Time]int, len(timeCol))
	for i, val := range timeCol {
		timestampToIdx[val] = i
	}

	timeSeries := make(map[string][]*float64)
	for _, met := range metrics {
		name := GetSeriesName(name, met.Labels)
		if _, ok := timeSeries[name]; !ok {
			timeSeries[name] = make([]*float64, len(timeCol))
		}
		colIdx := timestampToIdx[met.Timestamp]
		value := met.Value
		timeSeries[name][colIdx] = &value
	}

	fields := make([]*data.Field, 0, len(timeSeries)+1)
	for tsName, tsCol := range timeSeries {
		fields = append(fields, data.NewField(tsName, nil, tsCol))
	}
	fields = append(fields, data.NewField("time", nil, timeCol))
	return data.NewFrame("response", fields...)
}

func GetTimeColumn(metrics []TimeSeriesMetric) []time.Time {
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

func GetSeriesName(name string, labels map[string]string) string {
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
