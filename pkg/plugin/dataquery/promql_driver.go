package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana/pkg/promlib/converter"
	jsoniter "github.com/json-iterator/go"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"net/http"
	"net/url"
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
	return p.FutureExecute(ctx)
}

func (p *PromQlDriver) ExecuteDemo(ctx context.Context) backend.DataResponse {
	// TODO: Replace with an actual api call

	values := make(url.Values)
	values.Add("start", "1727271155")
	values.Add("language", "promql")
	values.Add("query", "http_in_flight_requests")
	values.Add("end", "1727385162")
	values.Add("step", "15")
	values.Add("table", "prometheusMsg_REALTIME")

	req, err := http.NewRequest(http.MethodGet, "http://pinot:8000/timeseries/api/v1/query_range?"+values.Encode(), nil)
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}

	iter := jsoniter.Parse(jsoniter.ConfigDefault, resp.Body, 1024)
	return converter.ReadPrometheusStyleResult(iter, converter.Options{})
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
