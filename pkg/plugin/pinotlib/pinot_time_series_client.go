package pinotlib

import (
	"context"
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/logger"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	TimeSeriesTableColumnMetricName  = "metric" // "metric"
	TimeSeriesTableColumnLabels      = "labels"
	TimeSeriesTableColumnMetricValue = "value"
	TimeSeriesTableColumnTimestamp   = "ts" // "ts"
	TimeSeriesTimestampFormat        = "1:MILLISECONDS:EPOCH"
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
	Table string
}

func (x *PinotTimeSeriesQuery) ToMap() map[string]string {
	return map[string]string{
		"query":    x.Query,
		"start":    x.formatTime(x.Start),
		"end":      x.formatTime(x.End),
		"step":     x.formatStep(x.Step),
		"language": "promql",
		// TODO: Set the table name correctly.
		"table": "prometheusMsg_REALTIME",
	}
}

func (x *PinotTimeSeriesQuery) ToUrlValues() url.Values {
	values := make(url.Values)
	for k, v := range x.ToMap() {
		values.Add(k, v)
	}
	return values
}

func (x *PinotTimeSeriesQuery) formatTime(t time.Time) string {
	//return strconv.FormatFloat(float64(t.Unix())+float64(t.Nanosecond())/1e9, 'f', -1, 64)
	return strconv.FormatInt(t.Unix(), 10)
}

func (x *PinotTimeSeriesQuery) formatStep(s time.Duration) string {
	//return strconv.FormatFloat(x.Step.Seconds(), 'f', -1, 64)
	step := int64(math.Max(1, x.Step.Seconds()))
	return strconv.FormatInt(step, 10)
}

func (p *PinotTimeSeriesClient) ExecuteTimeSeriesQuery(ctx context.Context, req *PinotTimeSeriesQuery) (*http.Response, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	reqUrl := p.properties.BrokerUrl + "/timeseries/api/v1/query_range?" + req.ToUrlValues().Encode()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqUrl, nil)

	logger.Logger.Info(fmt.Sprintf("pinot/http - Outgoing %s %s", httpReq.Method, reqUrl))

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

func IsTimeSeriesTableSchema(schema TableSchema) bool {
	var hasMetricField bool
	for _, fieldSpec := range schema.DimensionFieldSpecs {
		if fieldSpec.Name == TimeSeriesTableColumnMetricName && fieldSpec.DataType == DataTypeString {
			hasMetricField = true
			break
		}
	}
	if !hasMetricField {
		return false
	}

	var hasLabelsField bool
	for _, fieldSpec := range schema.DimensionFieldSpecs {
		if fieldSpec.Name == TimeSeriesTableColumnLabels && fieldSpec.DataType == DataTypeJson {
			hasLabelsField = true
			break
		}
	}
	if !hasLabelsField {
		return false
	}

	var hasValueField bool
	for _, fieldSpec := range schema.MetricFieldSpecs {
		if fieldSpec.Name == TimeSeriesTableColumnMetricValue && fieldSpec.DataType == DataTypeDouble {
			hasValueField = true
			break
		}
	}
	if !hasValueField {
		return false
	}

	var hasTsField bool
	for _, fieldSpec := range schema.DateTimeFieldSpecs {
		if fieldSpec.Name == TimeSeriesTableColumnTimestamp && fieldSpec.DataType == DataTypeTimestamp {
			hasTsField = true
			break
		}
	}
	if !hasTsField {
		return false
	}

	return true
}
