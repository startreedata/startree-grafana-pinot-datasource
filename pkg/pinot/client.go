package pinot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
)

const (
	DefaultDatabase = "default"
)

type Client struct {
	properties ClientProperties
	headers    map[string]string
	httpClient *http.Client
	logger     Logger
}

type ClientProperties struct {
	ControllerUrl string
	BrokerUrl     string
	DatabaseName  string
	Authorization string
	QueryOptions  []QueryOption
}

type QueryOption struct {
	Name  string
	Value string
}

type Logger interface {
	Info(msg string, args ...interface{})
	Error(msg string, args ...interface{})
}

func NewPinotClient(httpClient *http.Client, properties ClientProperties) *Client {
	properties.BrokerUrl = strings.TrimSuffix(properties.BrokerUrl, "/")
	properties.ControllerUrl = strings.TrimSuffix(properties.ControllerUrl, "/")

	headers := make(map[string]string)
	if properties.Authorization != "" {
		headers["Authorization"] = properties.Authorization
	}
	if properties.DatabaseName != "" && properties.DatabaseName != DefaultDatabase {
		headers["Database"] = properties.DatabaseName
	}

	return &Client{
		properties: properties,
		headers:    headers,
		httpClient: httpClient,
		logger:     slog.Default(),
	}
}

func (p *Client) WithAuthorization(authorization string) *Client {
	properties := p.Properties()
	properties.Authorization = authorization
	return NewPinotClient(p.httpClient, properties)
}

func (p *Client) WithLogger(logger Logger) *Client {
	return &Client{
		properties: p.properties,
		headers:    p.headers,
		httpClient: p.httpClient,
		logger:     logger,
	}
}

func (p *Client) Properties() ClientProperties { return p.properties }

func (p *Client) newRequest(ctx context.Context, method string, url string, body io.Reader) (*http.Request, error) {
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

func (p *Client) doRequestAndDecodeResponse(req *http.Request, dest interface{}) error {
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
		return fmt.Errorf("pinot/http: Failed to decode response json: %w", err)
	}
	return nil
}

func (p *Client) doRequest(req *http.Request) (*http.Response, error) {
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("pinot/http: Request failed: %s %s %w", req.Method, req.URL.String(), err)
	}
	p.logger.Info("pinot/http: Outgoing http request completed.", "method", req.Method, "url", req.URL.String(), "status", resp.StatusCode)
	return resp, err
}

func (p *Client) closeResponseBody(ctx context.Context, resp *http.Response) {
	if err := resp.Body.Close(); err != nil {
		p.logger.Error("pinot/http: Failed to close response body.", "error", err)
	}
}

func (p *Client) newErrorFromResponseBody(ctx context.Context, resp *http.Response) error {
	var body bytes.Buffer
	if _, err := body.ReadFrom(resp.Body); err != nil {
		p.logger.Error("pinot/http: Failed to read response body.", "error", err)
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
