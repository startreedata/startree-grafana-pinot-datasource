package dataquery

import (
	"bytes"
	"context"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana/pkg/promlib/converter"
	jsoniter "github.com/json-iterator/go"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"io"
	"net/http"
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
		return NewDataInternalErrorResponse(err)
	}
	defer queryResponse.Body.Close()

	if queryResponse.StatusCode != http.StatusOK {
		var body bytes.Buffer
		_, _ = body.ReadFrom(queryResponse.Body)
		return NewDataInternalErrorResponse(fmt.Errorf("error executing promql query: %s", body.String()))
	}

	var buf bytes.Buffer
	io.Copy(&buf, queryResponse.Body)

	fmt.Println(buf.String())

	iter := jsoniter.Parse(jsoniter.ConfigDefault, &buf, 1024)
	dataResponse := converter.ReadPrometheusStyleResult(iter, converter.Options{})

	for _, frame := range dataResponse.Frames {
		p.decorateFrame(frame)
	}
	return dataResponse
}

func (p *PromQlDriver) decorateFrame(frame *data.Frame) {
	if len(frame.Fields) < 1 {
		return
	}
	frame.Fields[0].Config = &data.FieldConfig{Interval: float64(p.params.IntervalSize.Milliseconds())}

	for i := 1; i < len(frame.Fields); i++ {
		name := FormatSeriesName(p.params.Legend, frame.Fields[i].Labels)
		frame.Fields[i].SetConfig(&data.FieldConfig{DisplayNameFromDS: name})
	}
}
