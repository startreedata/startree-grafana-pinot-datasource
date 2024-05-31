package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
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

	listTablesCache     *ResourceCache[[]string]
	getTableSchemaCache *MultiResourceCache[string, TableSchema]
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
		listTablesCache:     NewResourceCache[[]string](5 * time.Minute),
		getTableSchemaCache: NewMultiResourceCache[string, TableSchema](5 * time.Minute),
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

func (p *PinotClient) ListTables(ctx context.Context) ([]string, error) {
	return p.listTablesCache.Get(ctx, p.controllerClient.ListTables)
}

func (p *PinotClient) GetTableSchema(ctx context.Context, tableName string) (TableSchema, error) {
	return p.getTableSchemaCache.Get(ctx, tableName, func(ctx context.Context) (TableSchema, error) {
		return p.controllerClient.GetTableSchema(ctx, tableName)
	})
}

type pinotControllerClient struct {
	properties PinotProperties
}

func (p *pinotControllerClient) ListTables(ctx context.Context) ([]string, error) {
	req, err := p.newControllerGetRequest(ctx, "/tables")
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

func (p *pinotControllerClient) GetTableSchema(ctx context.Context, tableName string) (TableSchema, error) {
	req, err := p.newControllerGetRequest(ctx, "/tables/"+url.PathEscape(tableName)+"/schema")
	if err != nil {
		return TableSchema{}, err
	}

	var schema TableSchema
	if err = p.doRequestAndDecodeResponse(req, &schema); err != nil {
		return TableSchema{}, err
	}
	return schema, nil
}

func (p *pinotControllerClient) newControllerGetRequest(ctx context.Context, endpoint string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.properties.ControllerUrl+endpoint, nil)
	if err != nil {
		// Realistically, this should never throw an error, but pass it through anyway.
		return nil, err
	}

	req.Header.Set("Authorization", p.properties.Authorization)
	req.Header.Set("Accept", "application/json")
	return req, err
}

func (p *pinotControllerClient) doRequestAndDecodeResponse(req *http.Request, dest interface{}) error {
	Logger.Info("pinot/http GET ", req.URL.String())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("pinot/http request failed: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			Logger.Error("pinot/http failed to close response body: ", err.Error())
		}
	}()

	if resp.StatusCode != http.StatusOK {
		var body bytes.Buffer
		if _, err := body.ReadFrom(resp.Body); err != nil {
			Logger.Error("pinot/http failed to read response body: ", err.Error())
		}
		return newControllerStatusError(resp.StatusCode, body.String())
	}

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
