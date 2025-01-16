package pinotlib

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/cache"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	DefaultDatabase = "default"
)

type PinotClient struct {
	properties PinotClientProperties
	headers    map[string]string
	httpClient *http.Client

	listDatabasesCache    *cache.ResourceCache[[]string]
	listTablesCache       *cache.ResourceCache[[]string]
	listTableConfigsCache *cache.MultiResourceCache[string, ListTableConfigsResponse]
	getTableSchemaCache   *cache.MultiResourceCache[string, TableSchema]
	getTableMetadataCache *cache.MultiResourceCache[string, TableMetadata]
	timeseriesLabelsCache *cache.MultiResourceCache[string, LabelsCollection]
	brokerQueryCache      *cache.MultiResourceCache[string, *BrokerResponse]

	brokerLimiter *Limiter
}

type PinotClientProperties struct {
	ControllerUrl string
	BrokerUrl     string
	DatabaseName  string
	Authorization string
	QueryOptions  []QueryOption

	ControllerCacheTimeout time.Duration
	BrokerCacheTimeout     time.Duration

	BrokerMaxQueryRate time.Duration
}

type QueryOption struct {
	Name  string
	Value string
}

type Limiter struct {
	ticker *time.Ticker
}

func NewLimiter(every time.Duration) *Limiter {
	var ticker *time.Ticker
	if every > 0 {
		ticker = time.NewTicker(every)
	}
	return &Limiter{ticker: ticker}
}

func (x *Limiter) Do(f func()) {
	if x.ticker != nil {
		<-x.ticker.C
	}
	f()
}

func (x *Limiter) Close() {
	if x.ticker != nil {
		x.ticker.Stop()
	}
}

var clientMap sync.Map

func NewPinotClient(properties PinotClientProperties) *PinotClient {
	key := fmt.Sprintf("%v", properties)
	val, ok := clientMap.Load(key)
	if ok {
		return val.(*PinotClient)
	}

	properties.BrokerUrl = strings.TrimSuffix(properties.BrokerUrl, "/")
	properties.ControllerUrl = strings.TrimSuffix(properties.ControllerUrl, "/")

	headers := make(map[string]string)
	if properties.Authorization != "" {
		headers["Authorization"] = properties.Authorization
	}
	if properties.DatabaseName != "" && properties.DatabaseName != DefaultDatabase {
		headers["Database"] = properties.DatabaseName
	}

	client := &PinotClient{
		properties: properties,
		headers:    headers,
		httpClient: http.DefaultClient,

		listDatabasesCache:    cache.NewResourceCache[[]string](properties.ControllerCacheTimeout),
		listTablesCache:       cache.NewResourceCache[[]string](properties.ControllerCacheTimeout),
		listTableConfigsCache: cache.NewMultiResourceCache[string, ListTableConfigsResponse](properties.ControllerCacheTimeout),
		getTableSchemaCache:   cache.NewMultiResourceCache[string, TableSchema](properties.ControllerCacheTimeout),
		getTableMetadataCache: cache.NewMultiResourceCache[string, TableMetadata](properties.ControllerCacheTimeout),
		timeseriesLabelsCache: cache.NewMultiResourceCache[string, LabelsCollection](properties.ControllerCacheTimeout),
		brokerQueryCache:      cache.NewMultiResourceCache[string, *BrokerResponse](properties.BrokerCacheTimeout),

		brokerLimiter: NewLimiter(properties.BrokerMaxQueryRate),
	}
	clientMap.Store(key, client)
	return client
}

func (p *PinotClient) Close() {
	p.brokerLimiter.Close()
}

func (p *PinotClient) Properties() PinotClientProperties { return p.properties }

func (p *PinotClient) newLogger(ctx context.Context) log.Logger {
	return log.FromContext(ctx).With("pinot-client", "pinot-http")
}

func (p *PinotClient) newLoggerWithError(ctx context.Context, err error) log.Logger {
	return log.FromContext(ctx).With("pinot-client", "pinot-http", "error", err)
}

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
	defer p.closeResponseBody(req.Context(), resp)

	if resp.StatusCode != http.StatusOK {
		return p.newErrorFromResponseBody(req.Context(), resp)
	}

	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	if err = decoder.Decode(&dest); err != nil {
		return fmt.Errorf("pinot/http failed to decode response json: %w", err)
	}
	return nil
}

func (p *PinotClient) doRequest(req *http.Request) (*http.Response, error) {
	p.newLogger(req.Context())
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("pinot/http: request failed: %s %s %w", req.Method, req.URL.String(), err)
	}
	p.newLogger(req.Context()).Info("Outgoing http request completed.", "method", req.Method, "url", req.URL.String(), "status", resp.StatusCode)
	return resp, err
}

func (p *PinotClient) closeResponseBody(ctx context.Context, resp *http.Response) {
	if err := resp.Body.Close(); err != nil {
		p.newLoggerWithError(ctx, err).Error("pinot/http: Failed to close response body.")
	}
}

func (p *PinotClient) newErrorFromResponseBody(ctx context.Context, resp *http.Response) error {
	var body bytes.Buffer
	if _, err := body.ReadFrom(resp.Body); err != nil {
		p.newLoggerWithError(ctx, err).Error("pinot/http: Failed to read response body.")
	}
	return newHttpStatusError(resp.StatusCode, body.String())
}

type HttpStatusError struct {
	StatusCode int
	Body       string
}

func (x *HttpStatusError) Error() string {
	return fmt.Sprintf("pinot/http non-200 response: (%d) %s", x.StatusCode, x.Body)
}

func IsHttpStatusError(err error) bool {
	var statusErr *HttpStatusError
	return errors.As(err, &statusErr)
}

func IsStatusNotFoundError(err error) bool {
	var statusErr *HttpStatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode == http.StatusNotFound
	}
	return false
}

func IsStatusForbiddenError(err error) bool {
	var statusErr *HttpStatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode == http.StatusForbidden
	}
	return false
}

func newHttpStatusError(statusCode int, body string) *HttpStatusError {
	return &HttpStatusError{
		StatusCode: statusCode,
		Body:       body,
	}
}
