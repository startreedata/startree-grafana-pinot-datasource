package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana/pkg/promlib/converter"
	jsoniter "github.com/json-iterator/go"
	"github.com/startree/pinot/pkg/plugin/pinotlib"
)

var _ Driver = &PromQlDriver{}

type PromQlDriver struct {
	client *pinotlib.PinotPromQlClient
}

func (p *PromQlDriver) Execute(ctx context.Context) backend.DataResponse {
	resp, err := p.client.Query(ctx, new(pinotlib.PinotPromQlRequest))
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}

	iter := jsoniter.Parse(jsoniter.ConfigDefault, resp.Body, 1024)
	return converter.ReadPrometheusStyleResult(iter, converter.Options{})
}
