package pinotlib

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/startree/pinot/pkg/plugin/logger"
	"github.com/startree/pinot/pkg/plugin/resources/cache"
	"github.com/startreedata/pinot-client-go/pinot"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const DefaultDatabase = "default"

type PinotClient struct {
	properties       PinotClientProperties
	brokerConn       *pinot.Connection
	controllerClient PinotControllerClient

	listDatabasesCache  *cache.ResourceCache[[]string]
	listTablesCache     *cache.ResourceCache[[]string]
	getTableSchemaCache *cache.MultiResourceCache[string, TableSchema]
}

type PinotClientProperties struct {
	ControllerUrl string
	BrokerUrl     string
	DatabaseName  string
	Authorization string

	ControllerCacheTimeout time.Duration
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

	return &PinotClient{
		properties: properties,
		brokerConn: brokerConn,
		controllerClient: PinotControllerClient{
			properties: properties,
			headers:    headers,
			httpClient: http.DefaultClient,
		},

		listDatabasesCache:  cache.NewResourceCache[[]string](properties.ControllerCacheTimeout),
		listTablesCache:     cache.NewResourceCache[[]string](properties.ControllerCacheTimeout),
		getTableSchemaCache: cache.NewMultiResourceCache[string, TableSchema](properties.ControllerCacheTimeout),
	}, nil
}

func (p *PinotClient) Properties() PinotClientProperties { return p.properties }

func (p *PinotClient) ExecuteSQL(ctx context.Context, table string, query string) (*pinot.BrokerResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	logger.Logger.Info(fmt.Sprintf("pinot/http: executing sql query: %s", query))
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

func (p *PinotClient) ListTables(ctx context.Context) ([]string, error) {
	return p.listTablesCache.Get(func() ([]string, error) {
		return p.controllerClient.ListTables(ctx)
	})
}

func (p *PinotClient) GetTableSchema(ctx context.Context, table string) (TableSchema, error) {
	return p.getTableSchemaCache.Get(table, func() (TableSchema, error) {
		return p.controllerClient.GetTableSchema(ctx, table)
	})
}

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
