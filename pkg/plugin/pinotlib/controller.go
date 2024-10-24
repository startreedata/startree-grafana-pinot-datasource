package pinotlib

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

func (p *PinotClient) ListDatabases(ctx context.Context) ([]string, error) {
	return p.listDatabasesCache.Get(func() ([]string, error) {
		req, err := p.newControllerGetRequest(ctx, "/databases")
		if err != nil {
			return nil, err
		}

		var databases []string
		err = p.doRequestAndDecodeResponse(req, &databases, false)
		if IsStatusNotFoundError(err) {
			return []string{}, nil
		} else if err != nil {
			return nil, err
		}
		return databases, nil
	})
}

func (p *PinotClient) ListTables(ctx context.Context) ([]string, error) {
	return p.listTablesCache.Get(func() ([]string, error) {
		endpoint := p.listTablesEndpoint(ctx)
		req, err := p.newControllerGetRequest(ctx, endpoint)
		if err != nil {
			return nil, err
		}

		var tablesResp struct {
			Tables []string `json:"tables"`
		}
		if err = p.doRequestAndDecodeResponse(req, &tablesResp, false); err != nil {
			return nil, err
		}

		tables := make([]string, len(tablesResp.Tables))
		databasePrefix := fmt.Sprintf("%s.", p.properties.DatabaseName)
		for i := range tablesResp.Tables {
			tables[i] = strings.TrimPrefix(tablesResp.Tables[i], databasePrefix)
		}
		return tables, nil
	})
}

func (p *PinotClient) listTablesEndpoint(ctx context.Context) string {
	req, err := p.newControllerHeadRequest(ctx, "/mytables")
	if err != nil {
		return "/tables"
	}

	resp, err := p.doRequest(req)
	if err != nil || resp.StatusCode == http.StatusNotFound {
		return "/tables"
	}
	return "/mytables"
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

func (p *PinotClient) GetTableSchema(ctx context.Context, table string) (TableSchema, error) {
	return p.getTableSchemaCache.Get(table, func() (TableSchema, error) {
		req, err := p.newControllerGetRequest(ctx, "/tables/"+url.PathEscape(table)+"/schema")
		if err != nil {
			return TableSchema{}, err
		}

		var schema TableSchema
		if err = p.doRequestAndDecodeResponse(req, &schema, false); err != nil {
			return TableSchema{}, err
		}
		return schema, nil
	})
}

type TableMetadata struct {
	TableNameAndType string `json:"tableName"`
	DiskSizeInBytes  uint64 `json:"diskSizeInBytes"`
	NumSegments      uint64 `json:"numSegments"`
	NumRows          uint64 `json:"numRows"`
}

func (p *PinotClient) GetTableMetadata(ctx context.Context, table string) (TableMetadata, error) {
	return p.getTableMetadataCache.Get(table, func() (TableMetadata, error) {
		req, err := p.newControllerGetRequest(ctx, "/tables/"+url.PathEscape(table)+"/metadata")
		if err != nil {
			return TableMetadata{}, err
		}
		var metadata TableMetadata
		if err = p.doRequestAndDecodeResponse(req, &metadata, false); err != nil {
			return TableMetadata{}, err
		}
		return metadata, nil
	})
}

func (p *PinotClient) newControllerHeadRequest(ctx context.Context, endpoint string) (*http.Request, error) {
	return p.newRequest(ctx, http.MethodHead, p.properties.ControllerUrl+endpoint, nil)
}

func (p *PinotClient) newControllerGetRequest(ctx context.Context, endpoint string) (*http.Request, error) {
	req, err := p.newRequest(ctx, http.MethodGet, p.properties.ControllerUrl+endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	return req, nil
}
