package pinotlib

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/startree/pinot/pkg/plugin/logger"
	"net/http"
	"strconv"
	"time"
)

type PinotPromQlClient struct {
	httpClient *http.Client
	properties PinotClientProperties
}

type PinotPromQlRequest struct {
	Query string
	Start time.Time
	End   time.Time
	Step  time.Duration
}

func (x *PinotPromQlRequest) MarshalJSON() ([]byte, error) {
	formatTime := func(t time.Time) string {
		return strconv.FormatFloat(float64(t.Unix())+float64(t.Nanosecond())/1e9, 'f', -1, 64)
	}

	return json.Marshal(map[string]string{
		"query": x.Query,
		"start": formatTime(x.Start),
		"end":   formatTime(x.End),
		"step":  strconv.FormatFloat(x.Step.Seconds(), 'f', -1, 64),
	})
}

func (p *PinotPromQlClient) Query(ctx context.Context, req *PinotPromQlRequest) (*http.Response, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(req); err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.properties.BrokerUrl+"/timeseries", &buf)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Add("Authorization", p.properties.Authorization)
	httpReq.Header.Add("Content-Type", "application/json")

	logger.Logger.Info(fmt.Sprintf("pinot/http: executing promql query: %s", req.Query))

	return p.httpClient.Do(httpReq)
}
