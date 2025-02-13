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
}

func (params PromQlQuery) Execute(client *pinotlib.PinotClient, ctx context.Context) backend.DataResponse {
	if strings.TrimSpace(params.PromQlCode) == "" {
		return NewEmptyDataResponse()
	}

	queryResponse, err := client.ExecuteTimeSeriesQuery(ctx, &pinotlib.TimeSeriesRangeQuery{
		Language:  pinotlib.TimeSeriesQueryLanguagePromQl,
		Query:     params.PromQlCode,
		Start:     params.TimeRange.From,
		End:       params.TimeRange.To,
		Step:      params.IntervalSize,
		TableName: params.TableName,
	})
	if err != nil {
		// TODO: Separate downstream and plugin errors.
		return NewPluginErrorResponse(err)
	}

	frames := extractTimeSeriesMatrix(queryResponse.Data.Result, params.Legend, params.IntervalSize, 1)
	return NewOkDataResponse(frames...)
}

func extractTimeSeriesMatrix(results []pinotlib.TimeSeriesResult, legend string, intervalSize time.Duration, limit int) []*data.Frame {
	var legendFormatter LegendFormatter

	var seriesCount int
	if limit < 0 {
		seriesCount = len(results)
	} else {
		seriesCount = min(limit, len(results))
	}
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
