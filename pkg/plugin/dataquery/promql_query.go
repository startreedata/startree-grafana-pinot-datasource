package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"strings"
	"time"
)

type PromQlQuery struct {
	TableName    string
	PromQlCode   string
	TimeRange    TimeRange
	IntervalSize time.Duration
	Legend       string
}

func (params PromQlQuery) Execute(ctx context.Context, client *pinotlib.PinotClient) backend.DataResponse {
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

	frames := ExtractTimeSeriesMatrix(queryResponse.Data.Result, params.Legend, params.IntervalSize)
	return NewOkDataResponse(frames...)
}

func ExtractTimeSeriesMatrix(results []pinotlib.TimeSeriesResult, legend string, intervalSize time.Duration) []*data.Frame {
	var legendFormatter LegendFormatter

	frames := make([]*data.Frame, len(results))
	for i, res := range results {
		tsField := data.NewField("time", nil, res.Timestamps).SetConfig(&data.FieldConfig{
			Interval: float64(intervalSize.Milliseconds())})
		metField := data.NewField("", res.Metric, res.Values).SetConfig(&data.FieldConfig{
			DisplayNameFromDS: legendFormatter.FormatSeriesName(legend, res.Metric),
		})
		frames[i] = data.NewFrame("", tsField, metField)
	}
	return frames
}
