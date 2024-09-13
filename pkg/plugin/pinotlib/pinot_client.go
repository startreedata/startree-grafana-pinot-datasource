package pinotlib

import (
	"context"
	"fmt"
	"github.com/startreedata/pinot-client-go/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/logger"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/resources/cache"
	"net/http"
	"strings"
	"sync"
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
)

type PinotClient struct {
	properties       PinotClientProperties
	brokerConn       *pinot.Connection
	controllerClient PinotControllerClient
	PinotTimeSeriesClient

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

	httpClient := http.DefaultClient
	return &PinotClient{
		properties: properties,
		brokerConn: brokerConn,
		controllerClient: PinotControllerClient{
			properties: properties,
			headers:    headers,
			httpClient: httpClient,
		},
		PinotTimeSeriesClient: PinotTimeSeriesClient{
			properties: properties,
			headers:    headers,
			httpClient: httpClient,
		},

		listDatabasesCache:  cache.NewResourceCache[[]string](properties.ControllerCacheTimeout),
		listTablesCache:     cache.NewResourceCache[[]string](properties.ControllerCacheTimeout),
		getTableSchemaCache: cache.NewMultiResourceCache[string, TableSchema](properties.ControllerCacheTimeout),
	}, nil
}

func (p *PinotClient) Properties() PinotClientProperties { return p.properties }

func (p *PinotClient) ExecuteSQL(ctx context.Context, table string, query string) (*pinot.BrokerResponse, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
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

func (p *PinotClient) ListPromQlTables(ctx context.Context) ([]string, error) {
	// TODO: Eventually this functionality will be replaced with a pinot api

	allTables, err := p.ListTables(ctx)
	if err != nil {
		return nil, err
	}

	localCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, len(allTables))
	resultCh := make(chan string, len(allTables))
	defer cancel()
	defer close(errCh)
	defer close(resultCh)

	var wg sync.WaitGroup
	wg.Add(len(allTables))
	for _, table := range allTables {
		go func(table string) {
			schema, err := p.GetTableSchema(localCtx, table)
			if err != nil {
				cancel()
				errCh <- err
			} else if p.IsPromQlTableSchema(schema) {
				resultCh <- table
			}
			wg.Done()
		}(table)
	}
	wg.Wait()

	var results []string
	for {
		select {
		case table := <-resultCh:
			results = append(results, table)
		case err := <-errCh:
			return nil, err
		default:
			return results, nil
		}
	}
}

func (p *PinotClient) IsPromQlTableSchema(schema TableSchema) bool {
	type FieldSpec struct {
		Name     string `json:"name"`
		DataType string `json:"dataType"`
	}

	var hasMetricField bool
	for _, fieldSpec := range schema.DimensionFieldSpecs {
		if fieldSpec.Name == "metric" && fieldSpec.DataType == "STRING" {
			hasMetricField = true
			break
		}
	}
	if !hasMetricField {
		return false
	}

	var hasLabelsField bool
	for _, fieldSpec := range schema.DimensionFieldSpecs {
		if fieldSpec.Name == "labels" && fieldSpec.DataType == "JSON" {
			hasLabelsField = true
			break
		}
	}
	if !hasLabelsField {
		return false
	}

	var hasValueField bool
	for _, fieldSpec := range schema.MetricFieldSpecs {
		if fieldSpec.Name == "metric" && fieldSpec.DataType == "DOUBLE" {
			hasValueField = true
			break
		}
	}
	if !hasValueField {
		return false
	}

	var hasTsField bool
	for _, fieldSpec := range schema.DateTimeFieldSpecs {
		if fieldSpec.Name == "ts" && fieldSpec.DataType == "TIMESTAMP" {
			hasTsField = true
			break
		}
	}
	if !hasTsField {
		return false
	}

	return true
}

func (p *PinotClient) GetTableSchema(ctx context.Context, table string) (TableSchema, error) {
	return p.getTableSchemaCache.Get(table, func() (TableSchema, error) {
		return p.controllerClient.GetTableSchema(ctx, table)
	})
}
