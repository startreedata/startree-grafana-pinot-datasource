package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana/pkg/promlib/converter"
	jsoniter "github.com/json-iterator/go"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"time"
)

var _ Driver = &PromQlDriver{}

type PromQlDriverParams struct {
	PromQlCode   string
	TimeRange    TimeRange
	IntervalSize time.Duration
}

type PromQlDriver struct {
	client *pinotlib.PinotPromQlClient
	params PromQlDriverParams
}

func (p *PromQlDriver) Execute(ctx context.Context) backend.DataResponse {
	resp, err := p.client.Query(ctx, &pinotlib.PinotPromQlRequest{
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
