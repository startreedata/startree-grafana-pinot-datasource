package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"strings"
	"time"
)

var _ Driver = &PromQlDriver{}

type PromQlCodeDriverParams struct {
	PinotClient *pinotlib.PinotClient

	TableName    string
	PromQlCode   string
	TimeRange    TimeRange
	IntervalSize time.Duration
	Legend       string
}

type PromQlDriver struct {
	params PromQlCodeDriverParams
}

func NewPromQlCodeDriver(params PromQlCodeDriverParams) *PromQlDriver {
	return &PromQlDriver{
		params: params,
	}
}

func (p *PromQlDriver) Execute(ctx context.Context) backend.DataResponse {
	if strings.TrimSpace(p.params.PromQlCode) == "" {
		return NewEmptyDataResponse()
	}

	queryResponse, err := p.params.PinotClient.ExecuteTimeSeriesQuery(ctx, &pinotlib.TimeSeriesRangeQuery{
		Language:  pinotlib.TimeSeriesQueryLanguagePromQl,
		Query:     p.params.PromQlCode,
		Start:     p.params.TimeRange.From,
		End:       p.params.TimeRange.To,
		Step:      p.params.IntervalSize,
		TableName: p.params.TableName,
	})
	if err != nil {
		// TODO: Separate downstream and plugin errors.
		return NewPluginErrorResponse(err)
	}

	frames := ExtractTimeSeriesMatrix(queryResponse.Data.Result, p.params.Legend, p.params.IntervalSize)
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
