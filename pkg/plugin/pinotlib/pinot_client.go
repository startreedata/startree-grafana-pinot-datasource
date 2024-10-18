package pinotlib

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/startreedata/pinot-client-go/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/logger"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/resources/cache"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	DefaultDatabase = "default"

	// https://docs.pinot.apache.org/configuration-reference/schema
	DataTypeInt       = "INT"
	DataTypeLong      = "LONG"
	DataTypeFloat     = "FLOAT"
	DataTypeDouble    = "DOUBLE"
	DataTypeBoolean   = "BOOLEAN"
	DataTypeTimestamp = "TIMESTAMP"
	DataTypeString    = "STRING"
	DataTypeJson      = "JSON"
	DataTypeBytes     = "BYTES"

	TimeSeriesTableColumnMetricName  = "metric"
	TimeSeriesTableColumnLabels      = "labels"
	TimeSeriesTableColumnMetricValue = "value"
	TimeSeriesTableColumnTimestamp   = "ts"
	TimeSeriesTimestampFormat        = "1:MILLISECONDS:EPOCH"
	TimeSeriesQueryLanguagePromQl    = "promql"
)

type PinotClient struct {
	properties PinotClientProperties
	brokerConn *pinot.Connection
	headers    map[string]string
	httpClient *http.Client

	listDatabasesCache    *cache.ResourceCache[[]string]
	listTablesCache       *cache.ResourceCache[[]string]
	getTableSchemaCache   *cache.MultiResourceCache[string, TableSchema]
	getTableMetadataCache *cache.MultiResourceCache[string, TableMetadata]
	timeseriesLabelsCache *cache.MultiResourceCache[string, LabelsCollection]
}

type PinotClientProperties struct {
	ControllerUrl string
	BrokerUrl     string
	DatabaseName  string
	Authorization string

	ControllerCacheTimeout time.Duration
}

func NewPinotClient(properties PinotClientProperties) (*PinotClient, error) {
	properties.BrokerUrl = strings.TrimSuffix(properties.BrokerUrl, "/")
	properties.ControllerUrl = strings.TrimSuffix(properties.ControllerUrl, "/")

	headers := make(map[string]string)
	if properties.Authorization != "" {
		headers["Authorization"] = properties.Authorization
	}
	if properties.DatabaseName != "" && properties.DatabaseName != DefaultDatabase {
		headers["Database"] = properties.DatabaseName
	}

	brokerConn, err := pinot.NewWithConfig(&pinot.ClientConfig{
		BrokerList:      []string{properties.BrokerUrl},
		ExtraHTTPHeader: headers,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create broker client: %w", err)
	}

	httpClient := http.DefaultClient
	return &PinotClient{
		properties: properties,
		brokerConn: brokerConn,
		headers:    headers,
		httpClient: httpClient,

		listDatabasesCache:    cache.NewResourceCache[[]string](properties.ControllerCacheTimeout),
		listTablesCache:       cache.NewResourceCache[[]string](properties.ControllerCacheTimeout),
		getTableSchemaCache:   cache.NewMultiResourceCache[string, TableSchema](properties.ControllerCacheTimeout),
		getTableMetadataCache: cache.NewMultiResourceCache[string, TableMetadata](properties.ControllerCacheTimeout),
		timeseriesLabelsCache: cache.NewMultiResourceCache[string, LabelsCollection](properties.ControllerCacheTimeout),
	}, nil
}

func (p *PinotClient) Properties() PinotClientProperties { return p.properties }

func (p *PinotClient) newRequest(ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		// Realistically, this should never throw an error, but pass it through anyway.
		return nil, err
	}

	for k, v := range p.headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

func (p *PinotClient) doRequestAndDecodeResponse(req *http.Request, dest interface{}) error {
	resp, err := p.doRequest(req)
	if err != nil {
		return err
	}
	defer p.closeResponseBody(resp)

	if resp.StatusCode != http.StatusOK {
		return p.newErrorFromResponseBody(resp)
	}

	return p.decodeResponse(resp, dest)
}

func (p *PinotClient) doRequest(req *http.Request) (*http.Response, error) {
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("pinot/http request failed: %s %s %w", req.Method, req.URL.String(), err)
	}
	logger.Logger.Info(fmt.Sprintf("pinot/http %s %s %d", req.Method, req.URL.String(), resp.StatusCode))
	return resp, err
}

func (p *PinotClient) closeResponseBody(resp *http.Response) {
	if err := resp.Body.Close(); err != nil {
		logger.Logger.Error("pinot/http failed to close response body: ", err.Error())
	}
}

func (p *PinotClient) newErrorFromResponseBody(resp *http.Response) error {
	var body bytes.Buffer
	if _, err := body.ReadFrom(resp.Body); err != nil {
		logger.Logger.Error("pinot/http failed to read response body: ", err.Error())
	}
	return newHttpStatusError(resp.StatusCode, body.String())
}

func (p *PinotClient) decodeResponse(resp *http.Response, dest interface{}) error {
	if err := json.NewDecoder(resp.Body).Decode(dest); err != nil {
		return fmt.Errorf("pinot/http failed to decode response json: %w", err)
	}
	return nil
}

type HttpStatusError struct {
	StatusCode int
	Body       string
	Err        error
}

func (x *HttpStatusError) Error() string { return x.Err.Error() }

func IsControllerStatusError(err error, statusCode int) bool {
	if err == nil {
		return false
	}

	var statusErr *HttpStatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode == statusCode
	}
	return false
}

func newHttpStatusError(statusCode int, body string) *HttpStatusError {
	return &HttpStatusError{
		StatusCode: statusCode,
		Body:       body,
		Err:        fmt.Errorf("pinot/http non-200 response: (%d) %s", statusCode, body),
	}
}
