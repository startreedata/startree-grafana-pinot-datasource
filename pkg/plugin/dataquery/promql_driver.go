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
		return backend.DataResponse{}
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
		return NewPluginErrorResponse(err)
	}

	frames := ExtractTimeSeriesMatrix(queryResponse.Data.Result, p.params.Legend, p.params.IntervalSize)
	return NewOkDataResponse(frames...)
}

func (p *PromQlDriver) decorateFrame(frame *data.Frame) {
	if len(frame.Fields) < 1 {
		return
	}
	frame.Fields[0].Config = &data.FieldConfig{Interval: float64(p.params.IntervalSize.Milliseconds())}

	var formatter LegendFormatter
	for i := 1; i < len(frame.Fields); i++ {
		name := formatter.FormatSeriesName(p.params.Legend, frame.Fields[i].Labels)
		frame.Fields[i].SetConfig(&data.FieldConfig{DisplayNameFromDS: name})
	}
}
