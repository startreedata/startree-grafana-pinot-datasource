package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"math"
	"strings"
	"time"
)

var _ ExecutableQuery = PromQlQuery{}

type PromQlQuery struct {
	TableName    string
	PromQlCode   string
	TimeRange    TimeRange
	IntervalSize time.Duration
	StepOverride time.Duration
	Legend       string
	SeriesLimit  int
	// MaxDataPoints bounds the number of points per series when deriving the automatic step.
	MaxDataPoints int64
}

// minPromQlStep is the smallest step sent to the timeseries API.
const minPromQlStep = time.Second

// parseStepOverride returns zero for an empty or invalid override so the automatic interval is used.
func parseStepOverride(stepSize string) time.Duration {
	step, err := time.ParseDuration(strings.TrimSpace(stepSize))
	if err != nil || step <= 0 {
		return 0
	}
	return step
}

// resolveStep applies MaxDataPoints to the automatic step. Explicit overrides retain the
// timeseries API's existing whole-second resolution and one-second minimum.
func (query PromQlQuery) resolveStep() time.Duration {
	if query.StepOverride > 0 {
		return max(minPromQlStep, query.StepOverride.Truncate(time.Second))
	}

	step := max(minPromQlStep, query.IntervalSize)
	if span := query.TimeRange.To.Sub(query.TimeRange.From); query.MaxDataPoints > 0 && span > 0 {
		minStep := 1 + (span-1)/time.Duration(query.MaxDataPoints)
		step = max(step, minStep)
	}
	// Round up to whole seconds without overflowing a time.Duration.
	stepSeconds := (step-1)/time.Second + 1
	return min(stepSeconds, math.MaxInt64/time.Second) * time.Second
}

// alignToStep rounds t down to a multiple of step since the Unix epoch. (time.Truncate aligns to
// Go's zero time instead, which differs for steps that don't divide a day.)
func alignToStep(t time.Time, step time.Duration) time.Time {
	stepSeconds := int64(step / time.Second)
	unix := t.Unix()
	offset := unix % stepSeconds
	if offset < 0 {
		offset += stepSeconds
	}
	return time.Unix(unix-offset, 0).UTC()
}

func (query PromQlQuery) Execute(client *pinot.Client, ctx context.Context) backend.DataResponse {
	if strings.TrimSpace(query.PromQlCode) == "" {
		return NewEmptyDataResponse()
	}

	step := query.resolveStep()
	start, end := alignToStep(query.TimeRange.From, step), alignToStep(query.TimeRange.To, step)
	if !start.Before(end) {
		// Preserve the requested range when alignment would collapse it to zero buckets.
		start, end = query.TimeRange.From, query.TimeRange.To
	}
	queryResponse, err := client.ExecuteTimeSeriesQuery(ctx, &pinot.TimeSeriesRangeQuery{
		Language:  pinot.TimeSeriesQueryLanguagePromQl,
		Query:     query.PromQlCode,
		Start:     start,
		End:       end,
		Step:      step,
		TableName: query.TableName,
	})
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	frames := extractTimeSeriesMatrix(queryResponse.Data.Result, query.Legend, step, query.SeriesLimit)
	return NewOkDataResponse(frames...)
}

func extractTimeSeriesMatrix(results []pinot.TimeSeriesResult, legend string, intervalSize time.Duration, limit int) []*data.Frame {
	if limit < 1 {
		limit = DefaultSeriesLimit
	}

	var legendFormatter LegendFormatter
	frames := make([]*data.Frame, min(limit, len(results)))
	for i := range frames {
		tsField := data.NewField("time", nil, results[i].Timestamps).SetConfig(&data.FieldConfig{
			Interval: float64(intervalSize.Milliseconds())})
		metField := data.NewField("", results[i].Metric, results[i].Values).SetConfig(&data.FieldConfig{
			DisplayNameFromDS: legendFormatter.FormatSeriesName(legend, results[i].Metric),
		})
		frames[i] = data.NewFrame("", tsField, metField)
	}
	return frames
}
