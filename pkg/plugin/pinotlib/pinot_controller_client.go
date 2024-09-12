package pinotlib

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/logger"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type PinotControllerClient struct {
	properties PinotClientProperties
	headers    map[string]string
	httpClient *http.Client
}

func (p *PinotControllerClient) ListDatabases(ctx context.Context) ([]string, error) {
	req, err := p.newGetRequest(ctx, "/databases")
	if err != nil {
		return nil, err
	}

	resp, err := p.doRequest(req)
	if err != nil {
		return nil, err
	}
	defer p.closeResponseBody(resp)

	if resp.StatusCode == http.StatusNotFound {
		return []string{}, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, p.newErrorFromResponseBody(resp)
	}

	var databases []string
	if err = p.decodeResponse(resp, &databases); err != nil {
		return nil, err
	}
	return databases, nil
}

func (p *PinotControllerClient) ListTables(ctx context.Context) ([]string, error) {
	endpoint := p.listTablesEndpoint(ctx)
	req, err := p.newGetRequest(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	var tablesResp struct {
		Tables []string `json:"tables"`
	}
	if err = p.doRequestAndDecodeResponse(req, &tablesResp); err != nil {
		return nil, err
	}

	tables := make([]string, len(tablesResp.Tables))
	databasePrefix := fmt.Sprintf("%s.", p.properties.DatabaseName)
	for i := range tablesResp.Tables {
		tables[i] = strings.TrimPrefix(tablesResp.Tables[i], databasePrefix)
	}
	return tables, nil
}

func (p *PinotControllerClient) listTablesEndpoint(ctx context.Context) string {
	req, err := p.newHeadRequest(ctx, "/mytables")
	if err != nil {
		return "/tables"
	}

	resp, err := p.doRequest(req)
	if err != nil || resp.StatusCode == http.StatusNotFound {
		return "/tables"
	}
	return "/mytables"
}

func (p *PinotControllerClient) GetTableSchema(ctx context.Context, table string) (TableSchema, error) {
	req, err := p.newGetRequest(ctx, "/tables/"+url.PathEscape(table)+"/schema")
	if err != nil {
		return TableSchema{}, err
	}

	var schema TableSchema
	if err = p.doRequestAndDecodeResponse(req, &schema); err != nil {
		return TableSchema{}, err
	}
	return schema, nil
}

func (p *PinotControllerClient) newHeadRequest(ctx context.Context, endpoint string) (*http.Request, error) {
	return p.newRequest(ctx, http.MethodHead, endpoint, nil)
}

func (p *PinotControllerClient) newGetRequest(ctx context.Context, endpoint string) (*http.Request, error) {
	req, err := p.newRequest(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	return req, nil
}

func (p *PinotControllerClient) newRequest(ctx context.Context, method string, endpoint string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, p.properties.ControllerUrl+endpoint, body)
	if err != nil {
		// Realistically, this should never throw an error, but pass it through anyway.
		return nil, err
	}

	for k, v := range p.headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

func (p *PinotControllerClient) doRequestAndDecodeResponse(req *http.Request, dest interface{}) error {
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

func (p *PinotControllerClient) doRequest(req *http.Request) (*http.Response, error) {
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("pinot/http request failed: %s %s %w", req.Method, req.URL.String(), err)
	}
	logger.Logger.Info(fmt.Sprintf("pinot/http %s %s %d", req.Method, req.URL.String(), resp.StatusCode))
	return resp, err
}

func (p *PinotControllerClient) closeResponseBody(resp *http.Response) {
	if err := resp.Body.Close(); err != nil {
		logger.Logger.Error("pinot/http failed to close response body: ", err.Error())
	}
}

func (p *PinotControllerClient) newErrorFromResponseBody(resp *http.Response) error {
	var body bytes.Buffer
	if _, err := body.ReadFrom(resp.Body); err != nil {
		logger.Logger.Error("pinot/http failed to read response body: ", err.Error())
	}
	return newControllerStatusError(resp.StatusCode, body.String())
}

func (p *PinotControllerClient) decodeResponse(resp *http.Response, dest interface{}) error {
	if err := json.NewDecoder(resp.Body).Decode(dest); err != nil {
		return fmt.Errorf("pinot/http failed to decode response json: %w", err)
	}
	return nil
}

type ControllerStatusError struct {
	StatusCode int
	Body       string
	Err        error
}

func (x *ControllerStatusError) Error() string { return x.Err.Error() }

func IsControllerStatusError(err error, statusCode int) bool {
	if err == nil {
		return false
	}

	var controllerErr *ControllerStatusError
	if errors.As(err, &controllerErr) {
		return controllerErr.StatusCode == statusCode
	}
	return false
}

func newControllerStatusError(statusCode int, body string) *ControllerStatusError {
	return &ControllerStatusError{
		StatusCode: statusCode,
		Body:       body,
		Err:        fmt.Errorf("pinot/http non-200 response: (%d) %s", statusCode, body),
	}
}
