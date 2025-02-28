package pinot

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

func (p *Client) ListDatabases(ctx context.Context) ([]string, error) {
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
}

func (p *Client) ListTables(ctx context.Context) ([]string, error) {
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
}

func (p *Client) listTablesEndpoint(ctx context.Context) string {
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

func (p *Client) ListTableConfigs(ctx context.Context, table string) (ListTableConfigsResponse, error) {
	req, err := p.newControllerGetRequest(ctx, "/tables/"+url.PathEscape(table))
	if err != nil {
		return ListTableConfigsResponse{}, err
	}
	var data ListTableConfigsResponse
	if err = p.doRequestAndDecodeResponse(req, &data); err != nil {
		return ListTableConfigsResponse{}, err
	}
	return data, nil
}

// TableSchema is a JSON serializable Pinot table schema.
// Ref 	https://docs.pinot.apache.org/configuration-reference/schema.
type TableSchema struct {
	SchemaName          string               `json:"schemaName"`
	DimensionFieldSpecs []DimensionFieldSpec `json:"dimensionFieldSpecs"`
	MetricFieldSpecs    []MetricFieldSpec    `json:"metricFieldSpecs"`
	DateTimeFieldSpecs  []DateTimeFieldSpec  `json:"dateTimeFieldSpecs"`
	ComplexFieldSpecs   []ComplexFieldSpec   `json:"complexFieldSpecs,omitempty"`
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

type ComplexFieldSpec struct {
	Name            string          `json:"name"`
	DataType        string          `json:"dataType"`
	FieldType       string          `json:"fieldType"`
	NotNull         bool            `json:"notNull"`
	ChildFieldSpecs ChildFieldSpecs `json:"childFieldSpecs"`
}

type ChildFieldSpecs struct {
	Key   ChildFieldSpec `json:"key"`
	Value ChildFieldSpec `json:"value"`
}

type ChildFieldSpec struct {
	Name      string `json:"name"`
	DataType  string `json:"dataType"`
	FieldType string `json:"fieldType"`
	NotNull   bool   `json:"notNull"`
}

func (p *Client) GetTableSchema(ctx context.Context, table string) (TableSchema, error) {
	req, err := p.newControllerGetRequest(ctx, "/tables/"+url.PathEscape(table)+"/schema")
	if err != nil {
		return TableSchema{}, err
	}

	var schema TableSchema
	if err = p.doRequestAndDecodeResponse(req, &schema); err != nil {
		return TableSchema{}, err
	}
	return schema, nil
}

type TableMetadata struct {
	TableNameAndType string `json:"tableName"`
	DiskSizeInBytes  uint64 `json:"diskSizeInBytes"`
	NumSegments      uint64 `json:"numSegments"`
	NumRows          uint64 `json:"numRows"`
}

func (p *Client) GetTableMetadata(ctx context.Context, table string) (TableMetadata, error) {
	req, err := p.newControllerGetRequest(ctx, "/tables/"+url.PathEscape(table)+"/metadata")
	if err != nil {
		return TableMetadata{}, err
	}
	var metadata TableMetadata
	if err = p.doRequestAndDecodeResponse(req, &metadata); err != nil {
		return TableMetadata{}, err
	}
	return metadata, nil
}

func (p *Client) newControllerHeadRequest(ctx context.Context, endpoint string) (*http.Request, error) {
	return p.newRequest(ctx, http.MethodHead, p.properties.ControllerUrl+endpoint, nil)
}

func (p *Client) newControllerGetRequest(ctx context.Context, endpoint string) (*http.Request, error) {
	req, err := p.newRequest(ctx, http.MethodGet, p.properties.ControllerUrl+endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	return req, nil
}
