package pinotlib

import (
	"context"
	"fmt"
	"github.com/startreedata/pinot-client-go/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/logger"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/resources/cache"
	"net/http"
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
