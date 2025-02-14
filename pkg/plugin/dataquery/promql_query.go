package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
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

func (query PromQlQuery) Execute(client *pinotlib.PinotClient, ctx context.Context) backend.DataResponse {
	if strings.TrimSpace(query.PromQlCode) == "" {
		return NewEmptyDataResponse()
	}

	queryResponse, err := client.ExecuteTimeSeriesQuery(ctx, &pinotlib.TimeSeriesRangeQuery{
		Language:  pinotlib.TimeSeriesQueryLanguagePromQl,
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

func extractTimeSeriesMatrix(results []pinotlib.TimeSeriesResult, legend string, intervalSize time.Duration, limit int) []*data.Frame {
	var seriesCount int
	if limit == SeriesLimitDisabled {
		seriesCount = len(results)
	} else {
		seriesCount = min(limit, len(results))
	}

	var legendFormatter LegendFormatter
	frames := make([]*data.Frame, seriesCount)
	for i := 0; i < seriesCount; i++ {
		tsField := data.NewField("time", nil, results[i].Timestamps).SetConfig(&data.FieldConfig{
			Interval: float64(intervalSize.Milliseconds())})
		metField := data.NewField("", results[i].Metric, results[i].Values).SetConfig(&data.FieldConfig{
			DisplayNameFromDS: legendFormatter.FormatSeriesName(legend, results[i].Metric),
		})
		frames[i] = data.NewFrame("", tsField, metField)
	}
	return frames
}
