package pinotlib

import (
	"context"
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/logger"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type PinotTimeSeriesClient struct {
	properties PinotClientProperties
	headers    map[string]string
	httpClient *http.Client
}

type PinotTimeSeriesQuery struct {
	Query string
	Start time.Time
	End   time.Time
	Step  time.Duration
}

func (x *PinotTimeSeriesQuery) ToMap() map[string]string {
	return map[string]string{
		"query": x.Query,
		"start": x.formatTime(x.Start),
		"end":   x.formatTime(x.End),
		"step":  x.formatStep(x.Step),
	}
}

func (x *PinotTimeSeriesQuery) ToUrlValues() url.Values {
	var values url.Values
	for k, v := range x.ToMap() {
		values.Add(k, v)
	}
	return values
}

func (x *PinotTimeSeriesQuery) formatTime(t time.Time) string {
	return strconv.FormatFloat(float64(t.Unix())+float64(t.Nanosecond())/1e9, 'f', -1, 64)
}

func (x *PinotTimeSeriesQuery) formatStep(s time.Duration) string {
	return strconv.FormatFloat(x.Step.Seconds(), 'f', -1, 64)
}

func (p *PinotTimeSeriesClient) ExecuteTimeSeriesQuery(ctx context.Context, req *PinotTimeSeriesQuery) (*http.Response, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	reqUrl := p.properties.BrokerUrl + "/timeseries?" + req.ToUrlValues().Encode()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, reqUrl, nil)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Add("Authorization", p.properties.Authorization)
	for k, v := range p.headers {
		httpReq.Header.Set(k, v)
	}

	logger.Logger.Info(fmt.Sprintf("pinot/http: executing promql query: %s", req.Query))
	return p.httpClient.Do(httpReq)
}
