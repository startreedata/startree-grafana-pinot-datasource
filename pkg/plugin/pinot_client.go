package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startree/pinot/pkg/plugin/cache"
	"github.com/startreedata/pinot-client-go/pinot"
	"net/http"
	"net/url"
	"time"
)

type PinotProperties struct {
	ControllerUrl string `json:"controllerUrl"`
	BrokerUrl     string `json:"brokerUrl"`
	Authorization string `json:"-"` // Never serialize this field.
}

func PinotPropertiesFrom(settings backend.DataSourceInstanceSettings) (PinotProperties, error) {
	if settings.JSONData == nil {
		return PinotProperties{}, errors.New("data source json data is null")
	}

	var properties PinotProperties
	if err := json.Unmarshal(settings.JSONData, &properties); err != nil {
		return PinotProperties{}, fmt.Errorf("failed to unmarshal pinot properties: %w", err)
	} else if properties.BrokerUrl == "" {
		return PinotProperties{}, errors.New("broker url cannot be empty")
	} else if properties.ControllerUrl == "" {
		return PinotProperties{}, errors.New("controller url cannot be empty")
	}
	properties.Authorization = settings.DecryptedSecureJSONData["authToken"]
	return properties, nil
}

type TableSchema struct {
	SchemaName          string               `json:"schemaName"`
	DimensionFieldSpecs []DimensionFieldSpec `json:"dimensionFieldSpecs"`
	MetricFieldSpecs    []MetricFieldSpec    `json:"metricFieldSpecs"`
	DateTimeFieldSpecs  []DateTimeFieldSpec  `json:"dateTimeFieldSpecs"`
}

type DimensionFieldSpec struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
}

type MetricFieldSpec struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
}

type DateTimeFieldSpec struct {
	Name        string `json:"name"`
	DataType    string `json:"dataType"`
	Format      string `json:"format"`
	Granularity string `json:"granularity"`
}

type PinotClient struct {
	properties       PinotProperties
	brokerConn       *pinot.Connection
	controllerClient pinotControllerClient

	listDatabasesCache  *cache.ResourceCache[[]string]
	listTablesCache     *cache.MultiResourceCache[string, []string]
	getTableSchemaCache *cache.MultiResourceCache[string, TableSchema]
}

func NewPinotClient(properties PinotProperties) (*PinotClient, error) {
	headers := make(map[string]string)
	if properties.Authorization != "" {
		headers["Authorization"] = properties.Authorization
	}

	brokerConn, err := pinot.NewWithConfig(&pinot.ClientConfig{
		BrokerList:      []string{properties.BrokerUrl},
		ExtraHTTPHeader: headers,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create broker client: %w", err)
	}

	return &PinotClient{
		properties:          properties,
		brokerConn:          brokerConn,
		controllerClient:    pinotControllerClient{properties: properties},
		listDatabasesCache:  cache.NewResourceCache[[]string](5 * time.Minute),
		listTablesCache:     cache.NewMultiResourceCache[string, []string](5 * time.Minute),
		getTableSchemaCache: cache.NewMultiResourceCache[string, TableSchema](5 * time.Minute),
	}, nil
}

func (p *PinotClient) Properties() PinotProperties { return p.properties }

func (p *PinotClient) ExecuteSQL(ctx context.Context, table string, query string) (*pinot.BrokerResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	Logger.Info(fmt.Sprintf("pinot/http: executing sql query: %s", query))
	res, err := p.brokerConn.ExecuteSQL(table, query)
	if err != nil {
		return nil, err
	}

	if len(res.Exceptions) > 0 {
		return nil, fmt.Errorf(res.Exceptions[0].Message)
	}

	return res, nil
}

func (p *PinotClient) ListDatabases(ctx context.Context) ([]string, error) {
	return p.listDatabasesCache.Get(func() ([]string, error) {
		return p.controllerClient.ListDatabases(ctx)
	})
}

func (p *PinotClient) ListTables(ctx context.Context, database string) ([]string, error) {
	return p.listTablesCache.Get(database, func() ([]string, error) {
		return p.controllerClient.ListTables(ctx, database)
	})
}

func (p *PinotClient) GetTableSchema(ctx context.Context, database string, table string) (TableSchema, error) {
	return p.getTableSchemaCache.Get(database+"|"+table, func() (TableSchema, error) {
		return p.controllerClient.GetTableSchema(ctx, database, table)
	})
}

type pinotControllerClient struct {
	properties PinotProperties
}

func (p *pinotControllerClient) ListDatabases(ctx context.Context) ([]string, error) {
	req, err := p.newControllerGetRequest(ctx, "", "/databases")
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

	var databases []string
	if err = p.decodeResponse(resp, &databases); err != nil {
		return nil, err
	}
	return databases, nil
}

func (p *pinotControllerClient) ListTables(ctx context.Context, database string) ([]string, error) {
	req, err := p.newControllerGetRequest(ctx, database, "/tables")
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

func (p *pinotControllerClient) GetTableSchema(ctx context.Context, database string, table string) (TableSchema, error) {
	req, err := p.newControllerGetRequest(ctx, database, "/tables/"+url.PathEscape(table)+"/schema")
	if err != nil {
		return TableSchema{}, err
	}

	var schema TableSchema
	if err = p.doRequestAndDecodeResponse(req, &schema); err != nil {
		return TableSchema{}, err
	}
	return schema, nil
}

func (p *pinotControllerClient) newControllerGetRequest(ctx context.Context, database string, endpoint string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.properties.ControllerUrl+endpoint, nil)
	if err != nil {
		// Realistically, this should never throw an error, but pass it through anyway.
		return nil, err
	}

	req.Header.Set("Authorization", p.properties.Authorization)
	req.Header.Set("Accept", "application/json")
	if database != "" {
		req.Header.Set("Database", database)
	}
	return req, err
}

func (p *pinotControllerClient) doRequestAndDecodeResponse(req *http.Request, dest interface{}) error {
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

func (p *pinotControllerClient) doRequest(req *http.Request) (*http.Response, error) {
	Logger.Info(fmt.Sprintf("pinot/http %s %s", req.Method, req.URL.String()))
	resp, err := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
	}
	return resp, err
}

func (p *pinotControllerClient) closeResponseBody(resp *http.Response) {
	if err := resp.Body.Close(); err != nil {
		Logger.Error("pinot/http failed to close response body: ", err.Error())
	}
}

func (p *pinotControllerClient) newErrorFromResponseBody(resp *http.Response) error {
	var body bytes.Buffer
	if _, err := body.ReadFrom(resp.Body); err != nil {
		Logger.Error("pinot/http failed to read response body: ", err.Error())
	}
	return newControllerStatusError(resp.StatusCode, body.String())
}

func (p *pinotControllerClient) decodeResponse(resp *http.Response, dest interface{}) error {
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
