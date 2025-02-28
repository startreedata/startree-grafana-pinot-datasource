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
}

func (query PromQlQuery) Execute(client *pinot.Client, ctx context.Context) backend.DataResponse {
	if strings.TrimSpace(query.PromQlCode) == "" {
		return NewEmptyDataResponse()
	}

	queryResponse, err := client.ExecuteTimeSeriesQuery(ctx, &pinot.TimeSeriesRangeQuery{
		Language:  pinot.TimeSeriesQueryLanguagePromQl,
		Query:     query.PromQlCode,
		Start:     query.TimeRange.From,
		End:       query.TimeRange.To,
		Step:      query.IntervalSize,
		TableName: query.TableName,
	})
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	frames := extractTimeSeriesMatrix(queryResponse.Data.Result, query.Legend, query.IntervalSize, query.SeriesLimit)
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
