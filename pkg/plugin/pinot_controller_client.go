package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type PinotControllerClient struct {
	properties PinotClientProperties
}

func (p *PinotControllerClient) ListDatabases(ctx context.Context) ([]string, error) {
	req, err := p.newGetRequest(ctx, "", "/databases")
	if err != nil {
		return nil, err
	}

	resp, err := p.doRequest(req)
	if err != nil {
		return nil, fmt.Errorf("pinot/http request failed: %w", err)
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

func (p *PinotControllerClient) ListTables(ctx context.Context, database string) ([]string, error) {
	endpoint := p.listTablesEndpoint(ctx, database)
	req, err := p.newGetRequest(ctx, database, endpoint)
	if err != nil {
		return nil, err
	}

	var tablesResp struct {
		Tables []string `json:"tables"`
	}
	if err = p.doRequestAndDecodeResponse(req, &tablesResp); err != nil {
		return nil, err
	}

	return tablesResp.Tables, nil
}

func (p *PinotControllerClient) listTablesEndpoint(ctx context.Context, database string) string {
	req, err := p.newHeadRequest(ctx, database, "/mytables")
	if err != nil {
		return "/tables"
	}

	resp, err := p.doRequest(req)
	if err != nil || resp.StatusCode == http.StatusNotFound {
		return "/tables"
	}
	return "/mytables"
}

func (p *PinotControllerClient) GetTableSchema(ctx context.Context, database string, table string) (TableSchema, error) {
	req, err := p.newGetRequest(ctx, database, "/tables/"+url.PathEscape(table)+"/schema")
	if err != nil {
		return TableSchema{}, err
	}

	var schema TableSchema
	if err = p.doRequestAndDecodeResponse(req, &schema); err != nil {
		return TableSchema{}, err
	}
	return schema, nil
}

func (p *PinotControllerClient) newHeadRequest(ctx context.Context, database string, endpoint string) (*http.Request, error) {
	return p.newRequest(ctx, database, http.MethodHead, endpoint, nil)
}

func (p *PinotControllerClient) newGetRequest(ctx context.Context, database string, endpoint string) (*http.Request, error) {
	return p.newRequest(ctx, database, http.MethodGet, endpoint, nil)
}

func (p *PinotControllerClient) newRequest(ctx context.Context, database string, method string, endpoint string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, p.properties.ControllerUrl+endpoint, body)
	if err != nil {
		// Realistically, this should never throw an error, but pass it through anyway.
		return nil, err
	}

	if p.properties.Authorization != "" {
		req.Header.Set("Authorization", p.properties.Authorization)
	}
	if database != "" {
		req.Header.Set("Database", database)
	}
	req.Header.Set("Accept", "application/json")

	return req, err
}

func (p *PinotControllerClient) doRequestAndDecodeResponse(req *http.Request, dest interface{}) error {
	resp, err := p.doRequest(req)
	if err != nil {
		return fmt.Errorf("pinot/http request failed: %w", err)
	}
	defer p.closeResponseBody(resp)

	if resp.StatusCode != http.StatusOK {
		return p.newErrorFromResponseBody(resp)
	}

	return p.decodeResponse(resp, dest)
}

func (p *PinotControllerClient) doRequest(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultClient.Do(req)
	Logger.Info(fmt.Sprintf("pinot/http %s %s %d", req.Method, req.URL.String(), resp.StatusCode))
	return resp, err
}

func (p *PinotControllerClient) closeResponseBody(resp *http.Response) {
	if err := resp.Body.Close(); err != nil {
		Logger.Error("pinot/http failed to close response body: ", err.Error())
	}
}

func (p *PinotControllerClient) newErrorFromResponseBody(resp *http.Response) error {
	var body bytes.Buffer
	if _, err := body.ReadFrom(resp.Body); err != nil {
		Logger.Error("pinot/http failed to read response body: ", err.Error())
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

func IsControllerStatus(err error, statusCode int) bool {
	var u *ControllerStatusError
	ok := errors.As(err, &u)
	return ok && u.StatusCode == statusCode
}

func newControllerStatusError(statusCode int, body string) *ControllerStatusError {
	return &ControllerStatusError{
		StatusCode: statusCode,
		Body:       body,
		Err:        fmt.Errorf("pinot/http non-200 response: (%d) %s", statusCode, body),
	}
}
