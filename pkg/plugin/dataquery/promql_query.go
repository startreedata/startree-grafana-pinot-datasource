package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"strings"
	"time"
)

var _ ExecutableQuery = PromQlQuery{}

type PromQlQuery struct {
	TableName    string
	PromQlCode   string
	TimeRange    TimeRange
	IntervalSize time.Duration
	Legend       string
	SeriesLimit  int
	// MaxDataPoints bounds the number of points per series when deriving the step.
	MaxDataPoints int64
}

// minPromQlStep is the smallest step sent to the timeseries API.
const minPromQlStep = time.Second

// resolveStep returns the query step: the configured interval, raised if needed so the range yields
// at most MaxDataPoints points, and never below one second. Grafana sends a zero interval from some
// callers (alerting, API), which previously degraded to a 1s step over the whole range.
func (query PromQlQuery) resolveStep() time.Duration {
	step := query.IntervalSize
	if query.MaxDataPoints > 0 {
		if minStep := query.TimeRange.To.Sub(query.TimeRange.From) / time.Duration(query.MaxDataPoints); step < minStep {
			step = minStep
		}
	}
	if step < minPromQlStep {
		step = minPromQlStep
	}
	return step.Truncate(time.Second)
}

// alignToStep rounds t down to a multiple of step since the Unix epoch. (time.Truncate aligns to
// Go's zero time instead, which differs for steps that don't divide a day.)
func alignToStep(t time.Time, step time.Duration) time.Time {
	secs := int64(step / time.Second)
	if secs <= 0 {
		return t
	}
	return time.Unix(t.Unix()/secs*secs, 0).UTC()
}

func (query PromQlQuery) Execute(client *pinot.Client, ctx context.Context) backend.DataResponse {
	if strings.TrimSpace(query.PromQlCode) == "" {
		return NewEmptyDataResponse()
	}

	step := query.resolveStep()
	queryResponse, err := client.ExecuteTimeSeriesQuery(ctx, &pinot.TimeSeriesRangeQuery{
		Language: pinot.TimeSeriesQueryLanguagePromQl,
		Query:    query.PromQlCode,
		// Align start to the step like Grafana's Prometheus datasource, so points land on stable
		// timestamps as the dashboard refreshes.
		Start:     alignToStep(query.TimeRange.From, step),
		End:       query.TimeRange.To,
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
