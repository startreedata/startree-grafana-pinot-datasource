package pinotlib

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
		err = p.doRequestAndDecodeResponse(req, &databases)
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
		if err = p.doRequestAndDecodeResponse(req, &tablesResp); err != nil {
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

type TableType string

const TableTypeRealTime TableType = "REALTIME"
const TableTypeOffline TableType = "OFFLINE"

type ListTableConfigsResponse map[TableType]TableConfig

type TableConfig struct {
	TableName string    `json:"tableName"`
	TableType TableType `json:"tableType"`
	Query     struct {
		ExpressionOverrideMap map[string]string `json:"expressionOverrideMap"`
	} `json:"query"`
	IngestionConfig IngestionConfig `json:"ingestionConfig"`
}

type IngestionConfig struct {
	TransformConfigs []TransformConfig `json:"transformConfigs"`
}

type TransformConfig struct {
	ColumnName        string `json:"columnName"`
	TransformFunction string `json:"transformFunction"`
}

func (p *PinotClient) ListTableConfigs(ctx context.Context, table string) (ListTableConfigsResponse, error) {
	return p.listTableConfigsCache.Get(table, func() (ListTableConfigsResponse, error) {
		req, err := p.newControllerGetRequest(ctx, "/tables/"+url.PathEscape(table))
		if err != nil {
			return ListTableConfigsResponse{}, err
		}
		var data ListTableConfigsResponse
		if err = p.doRequestAndDecodeResponse(req, &data); err != nil {
			return ListTableConfigsResponse{}, err
		}
		return data, nil
	})
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
		if err = p.doRequestAndDecodeResponse(req, &schema); err != nil {
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
		if err = p.doRequestAndDecodeResponse(req, &metadata); err != nil {
			return TableMetadata{}, err
		}
		return metadata, nil
	})
}

func (p *PinotClient) TableSchemaExists(ctx context.Context, schemaName string) (bool, error) {
	_, err := p.GetTableSchema(ctx, schemaName)
	switch {
	case IsStatusNotFoundError(err):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

func (p *PinotClient) CreateTableSchema(ctx context.Context, schema TableSchema) error {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(schema); err != nil {
		return err
	}

	req, err := p.newControllerRequest(ctx, http.MethodPost, "/schemas", &body)
	if err != nil {
		return err
	}

	resp, err := p.doRequestAndCheckStatus(req)
	if err != nil {
		return err
	}
	defer p.closeResponseBody(ctx, resp)
	return nil
}

func (p *PinotClient) DeleteTableSchema(ctx context.Context, schemaName string, missingOk bool) error {
	req, err := p.newControllerRequest(ctx, http.MethodDelete, "/schemas/"+url.PathEscape(schemaName), nil)

	expectStatuses := []int{http.StatusOK}
	if missingOk {
		expectStatuses = append(expectStatuses, http.StatusNotFound)
	}
	resp, err := p.doRequestAndCheckStatus(req, expectStatuses...)
	if err != nil {
		return err
	}
	defer p.closeResponseBody(ctx, resp)
	return nil
}

func (p *PinotClient) TableExists(ctx context.Context, tableName string) (bool, error) {
	_, err := p.ListTableConfigs(ctx, tableName)
	switch {
	case IsStatusNotFoundError(err):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

func (p *PinotClient) CreateTable(ctx context.Context, tableConfig TableConfig) error {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(tableConfig); err != nil {
		return err
	}

	req, err := p.newControllerRequest(ctx, http.MethodPost, "/tables", &body)
	if err != nil {
		return err
	}

	resp, err := p.doRequestAndCheckStatus(req)
	if err != nil {
		return err
	}
	defer p.closeResponseBody(ctx, resp)
	return nil
}

func (p *PinotClient) newControllerHeadRequest(ctx context.Context, endpoint string) (*http.Request, error) {
	return p.newControllerRequest(ctx, http.MethodHead, endpoint, nil)
}

func (p *PinotClient) newControllerGetRequest(ctx context.Context, endpoint string) (*http.Request, error) {
	return p.newControllerRequest(ctx, http.MethodGet, endpoint, nil)
}

func (p *PinotClient) newControllerRequest(ctx context.Context, method string, endpoint string, body io.Reader) (*http.Request, error) {
	req, err := p.newRequest(ctx, method, p.properties.ControllerUrl+endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	return req, nil
}

type ControllerError struct{}
