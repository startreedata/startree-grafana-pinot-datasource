package dataquery

import (
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/logger"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
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
	Labels    []MetricLabel
}

type MetricLabel struct {
	name  string
	value string
}

type MetricSeries struct {
	name   string
	values []*float64
	labels map[string]string
}

func ExtractTimeSeriesDataFrame(params TimeSeriesExtractorParams, results *pinotlib.ResultTable) (*data.Frame, error) {
	metrics, err := ExtractMetrics(results, params.TimeColumnAlias, params.TimeColumnFormat, params.MetricColumnAlias)
	if err != nil {
		return nil, err
	}

	timeCol, metricSeries := PivotToTimeSeries(metrics, params.Legend)
	slices.SortFunc(metricSeries, func(a, b MetricSeries) int {
		return strings.Compare(a.name, b.name)
	})

	fields := make([]*data.Field, 0, len(metricSeries)+1)
	for _, series := range metricSeries {
		field := data.NewField(params.MetricName, series.labels, series.values)
		field.SetConfig(&data.FieldConfig{
			DisplayNameFromDS: series.name,
		})
		fields = append(fields, field)
	}
	fields = append(fields, data.NewField("time", nil, timeCol))

	return data.NewFrame("response", fields...), nil
}

func ExtractMetrics(results *pinotlib.ResultTable, timeColumnAlias string, timeColumnFormat string, metricColumnAlias string) ([]Metric, error) {
	timeColIdx, err := pinotlib.GetColumnIdx(results, timeColumnAlias)
	if err != nil {
		return nil, err
	}

	timeCol, err := pinotlib.ExtractTimeColumn(results, timeColIdx, timeColumnFormat)
	if err != nil {
		return nil, err
	}

	metColIdx, err := pinotlib.GetColumnIdx(results, metricColumnAlias)
	if err != nil {
		return nil, err
	}
	metCol := pinotlib.ExtractDoubleColumn(results, metColIdx)

	dimensions := make(map[string][]string)
	for colIdx := 0; colIdx < len(results.DataSchema.ColumnNames); colIdx++ {
		if colIdx == timeColIdx || colIdx == metColIdx {
			continue
		}
		name, _ := pinotlib.GetColumnName(results, colIdx)
		dimensions[name] = pinotlib.ExtractStringColumn(results, colIdx)
	}

	dimensionNames := make([]string, 0, len(dimensions))
	for name := range dimensions {
		dimensionNames = append(dimensionNames, name)
	}
	sort.Strings(dimensionNames)

	metrics := make([]Metric, pinotlib.GetRowCount(results))
	for rowIdx := 0; rowIdx < pinotlib.GetRowCount(results); rowIdx++ {
		labels := make([]MetricLabel, len(dimensions))
		for i, name := range dimensionNames {
			labels[i] = MetricLabel{
				name:  name,
				value: dimensions[name][rowIdx],
			}
		}

		metrics[rowIdx] = Metric{
			Timestamp: timeCol[rowIdx],
			Value:     metCol[rowIdx],
			Labels:    labels,
		}
	}

	return metrics, nil
}

func PivotToTimeSeries(metrics []Metric, legend string) ([]time.Time, []MetricSeries) {
	timeCol := GetTimeColumn(metrics)

	timestampToIdx := make(map[time.Time]int, len(timeCol))
	for i, val := range timeCol {
		timestampToIdx[val] = i
	}

	timeSeriesMap := make(map[int]MetricSeries)
	var formatter LegendFormatter
	var seriesMapper SeriesMapper

	for _, met := range metrics {
		tsKey := seriesMapper.GetKey(met.Labels)
		if _, ok := timeSeriesMap[tsKey]; !ok {
			labels := make(map[string]string, len(met.Labels))
			for _, label := range met.Labels {
				labels[label.name] = label.value
			}
			timeSeriesMap[tsKey] = MetricSeries{
				name:   formatter.FormatSeriesName(legend, labels),
				values: make([]*float64, len(timeCol)),
				labels: labels,
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

type SeriesMapper struct {
	order  map[string]int
	caches []map[string]int
	next   int
}

// GetKey generates a unique key for a slice of metric labels.
// * The slice of labels must always be the same length.
// * The slice of labels must always have the same names.
// * The names must appear in the same order.
func (x *SeriesMapper) GetKey(labels []MetricLabel) int {
	if x.order == nil {
		x.order = make(map[string]int, len(labels))
		for i := range labels {
			x.order[labels[i].name] = i
		}
	}

	if len(labels) != len(x.order) {
		panic(fmt.Sprintf(
			"SeriesMapper.GetKey() called with different length of labels: %d (got) vs %d (want)", len(labels), len(x.order)))
	}

	for i := range labels {
		name := labels[i].name
		order, ok := x.order[name]
		if !ok {
			panic(fmt.Sprintf("SeriesMapper.GetKey() encountered new label `%s`", name))
		}

		if order != i {
			panic(fmt.Sprintf(
				"SeriesMapper.GetKey() called with different order for label `%s`: %d (got) vs %d (want)", name, i, order))
		}
	}

	if len(labels) == 0 {
		return 0
	}

	if x.caches == nil {
		x.caches = make([]map[string]int, 1)
	}
	if x.caches[0] == nil {
		x.caches[0] = make(map[string]int)
	}
	cache := x.caches[0]
	for i := 0; i < len(labels)-1; i++ {
		val := labels[i].value
		if _, ok := cache[val]; !ok {
			cache[val] = len(x.caches)
			x.caches = append(x.caches, make(map[string]int))
		}
		cache = x.caches[cache[val]]
	}

	finalVal := labels[len(labels)-1].value
	if _, ok := cache[finalVal]; !ok {
		cache[finalVal] = x.next
		x.next++
	}
	return cache[finalVal]
}

type LegendFormatter struct {
	regexpCache map[string]*regexp.Regexp
}

func (f *LegendFormatter) getRegexpFromCache(key string) *regexp.Regexp {
	if f.regexpCache == nil {
		f.regexpCache = make(map[string]*regexp.Regexp)
	}
	if re, ok := f.regexpCache[key]; ok {
		return re
	}
	pattern := fmt.Sprintf(`\{\{\s*%s\s*}}`, regexp.QuoteMeta(key))
	re, err := regexp.Compile(pattern)
	if err != nil {
		logger.Logger.Debug("Error compiling legend regex", err)
		return nil
	}
	f.regexpCache[key] = re
	return re
}

func (f *LegendFormatter) FormatSeriesName(legend string, labels map[string]string) string {
	legend = strings.TrimSpace(legend)
	if !strings.Contains(legend, "{{") {
		return legend
	}

	for key, val := range labels {
		re := f.getRegexpFromCache(key)
		if re == nil {
			continue
		}
		legend = re.ReplaceAllString(legend, val)
	}
	return legend
}
