package pinotlib

import (
	"context"
	"fmt"
	"github.com/startreedata/pinot-client-go/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/logger"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/resources/cache"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/templates"
	"net/http"
	"sort"
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

	listDatabasesCache    *cache.ResourceCache[[]string]
	listTablesCache       *cache.ResourceCache[[]string]
	getTableSchemaCache   *cache.MultiResourceCache[string, TableSchema]
	timeseriesLabelsCache *cache.MultiResourceCache[string, LabelsCollection]
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

		listDatabasesCache:    cache.NewResourceCache[[]string](properties.ControllerCacheTimeout),
		listTablesCache:       cache.NewResourceCache[[]string](properties.ControllerCacheTimeout),
		getTableSchemaCache:   cache.NewMultiResourceCache[string, TableSchema](properties.ControllerCacheTimeout),
		timeseriesLabelsCache: cache.NewMultiResourceCache[string, LabelsCollection](properties.ControllerCacheTimeout),
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

func (p *PinotClient) GetTableSchema(ctx context.Context, table string) (TableSchema, error) {
	return p.getTableSchemaCache.Get(table, func() (TableSchema, error) {
		return p.controllerClient.GetTableSchema(ctx, table)
	})
}

func (p *PinotClient) ListTimeSeriesTables(ctx context.Context) ([]string, error) {
	// TODO: Replace with pinot api call when implemented.

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
			} else if IsTimeSeriesTableSchema(schema) {
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

func (p *PinotClient) ListTimeSeriesMetrics(ctx context.Context, tableName string, from time.Time, to time.Time) ([]string, error) {
	timeExprBuilder, err := NewTimeExpressionBuilder(TimeSeriesTableColumnTimestamp, TimeSeriesTimestampFormat)
	if err != nil {
		return nil, err
	}

	sql, err := templates.RenderDistinctValuesSql(templates.DistinctValuesSqlParams{
		ColumnName:     TimeSeriesTableColumnMetricName,
		TableName:      tableName,
		TimeFilterExpr: timeExprBuilder.TimeFilterExpr(from, to),
		Limit:          templates.DistinctValuesLimit,
	})
	if err != nil {
		return nil, err
	}

	resp, err := p.ExecuteSQL(ctx, tableName, sql)
	metrics := ExtractStringColumn(resp.ResultTable, 0)
	return metrics, nil
}

func (p *PinotClient) ListTimeSeriesLabelNames(ctx context.Context, tableName string, from time.Time, to time.Time) ([]string, error) {
	collection, err := p.fetchTimeSeriesLabels(ctx, tableName, from, to)
	if err != nil {
		return nil, err
	}
	return collection.Names(), nil
}

func (p *PinotClient) ListTimeSeriesLabelValues(ctx context.Context, tableName string, labelName string, from time.Time, to time.Time) ([]string, error) {
	collection, err := p.fetchTimeSeriesLabels(ctx, tableName, from, to)
	if err != nil {
		return nil, err
	}
	return collection.Values(labelName), nil
}

// TODO: Is set really necessary here?

type LabelsCollection map[string]Set[string]

func (x LabelsCollection) Names() []string {
	names := make([]string, 0, len(x))
	for key := range x {
		names = append(names, key)
	}
	sort.Strings(names)
	return names
}

func (x LabelsCollection) Values(name string) []string {
	values := x[name].Values()
	sort.Strings(values)
	return values
}

func (x LabelsCollection) Add(name, value string) {
	if _, ok := x[name]; !ok {
		x[name] = NewSet[string](1)
	}
	x[name].Add(value)
}

func (p *PinotClient) fetchTimeSeriesLabels(ctx context.Context, tableName string, from time.Time, to time.Time) (LabelsCollection, error) {
	// TODO: This code can be removed once the pinot api is implemented.
	cacheKey := fmt.Sprintf("table=%s&from=%s&to=%s", tableName, from.Format(time.RFC3339), to.Format(time.RFC3339))
	return p.timeseriesLabelsCache.Get(cacheKey, func() (LabelsCollection, error) {
		timeExprBuilder, err := NewTimeExpressionBuilder(TimeSeriesTableColumnTimestamp, TimeSeriesTimestampFormat)
		if err != nil {
			return nil, err
		}

		sql, err := templates.RenderDistinctValuesSql(templates.DistinctValuesSqlParams{
			ColumnName:     TimeSeriesTableColumnLabels,
			TableName:      tableName,
			TimeFilterExpr: timeExprBuilder.TimeFilterExpr(from, to),
			Limit:          templates.DistinctValuesLimit,
		})
		if err != nil {
			return nil, err
		}

		resp, err := p.ExecuteSQL(ctx, tableName, sql)
		if err != nil {
			return nil, err
		}

		labelRecords, err := ExtractJsonColumn[map[string]string](resp.ResultTable, 0)
		if err != nil {
			return nil, err
		}

		collection := make(LabelsCollection, len(labelRecords))
		for _, label := range labelRecords {
			for k, v := range label {
				collection.Add(k, v)
			}
		}
		return collection, nil
	})
}

func (p *PinotClient) IsTimeSeriesTable(ctx context.Context, tableName string) (bool, error) {
	schema, err := p.GetTableSchema(ctx, tableName)
	if err != nil {
		return false, err
	}

	return IsTimeSeriesTableSchema(schema), nil
}
