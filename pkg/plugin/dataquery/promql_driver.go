package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana/pkg/promlib/converter"
	jsoniter "github.com/json-iterator/go"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"math"
	"time"
)

var _ Driver = &PromQlDriver{}

type PromQlCodeDriverParams struct {
	PinotClient *pinotlib.PinotClient

	TableName    string
	PromQlCode   string
	TimeRange    TimeRange
	IntervalSize time.Duration
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
	// TODO: Replace with an actual api call

	tsLen := int(p.params.TimeRange.To.Sub(p.params.TimeRange.From)/p.params.IntervalSize) + 1
	ts := make([]time.Time, tsLen)

	ts[0] = p.params.TimeRange.From
	for i := 1; i < tsLen; i++ {
		ts[i] = ts[i-1].Add(p.params.IntervalSize)
	}

	met := make([]float64, tsLen)
	for i := 0; i < tsLen; i++ {
		met[i] = math.Log(float64(i))
	}

	return backend.DataResponse{Frames: []*data.Frame{data.NewFrame("sample",
		data.NewField("ts", nil, ts),
		data.NewField("met", nil, met))}}
}

func (p *PromQlDriver) FutureExecute(ctx context.Context) backend.DataResponse {
	resp, err := p.params.PinotClient.ExecuteTimeSeriesQuery(ctx, &pinotlib.PinotTimeSeriesQuery{
		Query: p.params.PromQlCode,
		Start: p.params.TimeRange.From,
		End:   p.params.TimeRange.To,
		Step:  p.params.IntervalSize,
	})
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}

	iter := jsoniter.Parse(jsoniter.ConfigDefault, resp.Body, 1024)
	return converter.ReadPrometheusStyleResult(iter, converter.Options{})
}
