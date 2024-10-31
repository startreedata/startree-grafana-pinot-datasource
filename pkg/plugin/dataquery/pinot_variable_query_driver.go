package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/templates"
	"strings"
)

const (
	VariableQueryTypeTableList      = "TABLE_LIST"
	VariableQueryTypeColumnList     = "COLUMN_LIST"
	VariableQueryTypeDistinctValues = "DISTINCT_VALUES"
	VariableQueryTypePinotQlCode    = "PINOT_QL_CODE"

	ColumnTypeDateTime  = "DATETIME"
	ColumnTypeMetric    = "METRIC"
	ColumnTypeDimension = "DIMENSION"
	ColumnTypeAll       = "ALL"
)

type PinotVariableQueryParams struct {
	*pinotlib.PinotClient
	VariableType string
	TableName    string
	ColumnName   string
	ColumnType   string
	PinotQlCode  string
}

type PinotVariableQueryDriver struct {
	params PinotVariableQueryParams
}

func NewPinotVariableQueryDriver(params PinotVariableQueryParams) *PinotVariableQueryDriver {
	if params.ColumnType == "" {
		params.ColumnType = ColumnTypeAll
	}

	return &PinotVariableQueryDriver{params: params}
}

func (d *PinotVariableQueryDriver) Execute(ctx context.Context) backend.DataResponse {
	switch d.params.VariableType {
	case VariableQueryTypeTableList:
		return d.getTableList(ctx)
	case VariableQueryTypeColumnList:
		return d.getColumnList(ctx)
	case VariableQueryTypeDistinctValues:
		return d.getDistinctValues(ctx)
	case VariableQueryTypePinotQlCode:
		return d.getSqlResults(ctx)
	default:
		return NewEmptyDataResponse()
	}
}

func (d *PinotVariableQueryDriver) getSqlResults(ctx context.Context) backend.DataResponse {
	sqlCode := strings.TrimSpace(d.params.PinotQlCode)
	if sqlCode == "" {
		return NewEmptyDataResponse()
	}

	macroEngine := MacroEngine{
		TableName: d.params.TableName,
	}
	sqlCode, err := macroEngine.ExpandTableName(sqlCode)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, d.params.PinotClient, pinotlib.NewSqlQuery(sqlCode))
	if !ok {
		return backendResp
	}

	values := make([]string, results.RowCount()*results.ColumnCount())
	for colId := 0; colId < results.ColumnCount(); colId++ {
		for rowId, val := range pinotlib.ExtractStringColumn(results, colId) {
			// Extract values in table order.
			values[rowId*results.ColumnCount()+colId] = val
		}
	}
	values = pinotlib.GetDistinctValues(values)
	frame := data.NewFrame("result", data.NewField("codeValues", nil, values))
	return NewSqlQueryDataResponse(frame, exceptions)
}

func (d *PinotVariableQueryDriver) getDistinctValues(ctx context.Context) backend.DataResponse {
	if d.params.TableName == "" || d.params.ColumnName == "" {
		return NewEmptyDataResponse()
	}

	sql, err := templates.RenderDistinctValuesSql(templates.DistinctValuesSqlParams{
		ColumnName: d.params.ColumnName,
		TableName:  d.params.TableName,
	})
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, d.params.PinotClient, pinotlib.NewSqlQuery(sql))
	if !ok {
		return backendResp
	}

	values := pinotlib.ExtractStringColumn(results, 0)
	frame := data.NewFrame("result", data.NewField("distinctValues", nil, values))
	return NewSqlQueryDataResponse(frame, exceptions)
}

func (d *PinotVariableQueryDriver) getColumnList(ctx context.Context) backend.DataResponse {
	if d.params.TableName == "" {
		return NewEmptyDataResponse()
	}
	schema, err := d.params.PinotClient.GetTableSchema(ctx, d.params.TableName)
	if pinotlib.IsHttpStatusError(err) {
		return NewDownstreamErrorResponse(err)
	} else if err != nil {
		return NewPluginErrorResponse(err)
	}

	var columns []string
	if d.params.ColumnType == ColumnTypeAll || d.params.ColumnType == ColumnTypeDateTime {
		for _, spec := range schema.DateTimeFieldSpecs {
			columns = append(columns, spec.Name)
		}
	}
	if d.params.ColumnType == ColumnTypeAll || d.params.ColumnType == ColumnTypeMetric {
		for _, spec := range schema.MetricFieldSpecs {
			columns = append(columns, spec.Name)
		}
	}
	if d.params.ColumnType == ColumnTypeAll || d.params.ColumnType == ColumnTypeDimension {
		for _, spec := range schema.DimensionFieldSpecs {
			columns = append(columns, spec.Name)
		}
	}

	frame := data.NewFrame("result", data.NewField("columns", nil, columns))
	return NewOkDataResponse(frame)
}

func (d *PinotVariableQueryDriver) getTableList(ctx context.Context) backend.DataResponse {
	tables, err := d.params.PinotClient.ListTables(ctx)
	if pinotlib.IsHttpStatusError(err) {
		return NewDownstreamErrorResponse(err)
	} else if err != nil {
		return NewPluginErrorResponse(err)
	}
	frame := data.NewFrame("result", data.NewField("tables", nil, tables))
	return NewOkDataResponse(frame)
}
