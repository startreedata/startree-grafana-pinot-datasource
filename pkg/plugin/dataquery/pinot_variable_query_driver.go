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
		return NewDataResponse()
	}
}

func (d *PinotVariableQueryDriver) getSqlResults(ctx context.Context) backend.DataResponse {
	sqlCode := strings.TrimSpace(d.params.PinotQlCode)
	if sqlCode == "" {
		return NewDataResponse()
	}

	macroEngine := MacroEngine{
		TableName: d.params.TableName,
	}
	sqlCode, err := macroEngine.ExpandTableName(sqlCode)
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}

	resp, err := d.params.PinotClient.ExecuteSQL(ctx, d.params.TableName, sqlCode)
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}

	result := resp.ResultTable
	values := make([]string, result.GetRowCount()*result.GetColumnCount())
	for colId := 0; colId < result.GetColumnCount(); colId++ {
		for rowId, val := range ExtractStringColumn(result, colId) {
			// Extract values in table order.
			values[rowId*result.GetColumnCount()+colId] = val
		}
	}
	values = GetDistinctValues(values)
	frame := data.NewFrame("result", data.NewField("codeValues", nil, values))
	return NewDataResponse(frame)
}

func (d *PinotVariableQueryDriver) getDistinctValues(ctx context.Context) backend.DataResponse {
	if d.params.TableName == "" || d.params.ColumnName == "" {
		return NewDataResponse()
	}

	sql, err := templates.RenderDistinctValuesSql(templates.DistinctValuesSqlParams{
		ColumnName: d.params.ColumnName,
		TableName:  d.params.TableName,
	})
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}

	result, err := d.params.PinotClient.ExecuteSQL(ctx, d.params.TableName, sql)
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}

	values := ExtractStringColumn(result.ResultTable, 0)
	frame := data.NewFrame("result", data.NewField("distinctValues", nil, values))
	return NewDataResponse(frame)
}

func (d *PinotVariableQueryDriver) getColumnList(ctx context.Context) backend.DataResponse {
	if d.params.TableName == "" {
		return NewDataResponse()
	}
	schema, err := d.params.PinotClient.GetTableSchema(ctx, d.params.TableName)
	if err != nil {
		return NewDataInternalErrorResponse(err)
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
	return NewDataResponse(frame)
}

func (d *PinotVariableQueryDriver) getTableList(ctx context.Context) backend.DataResponse {
	tables, err := d.params.PinotClient.ListTables(ctx)
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}
	frame := data.NewFrame("result", data.NewField("tables", nil, tables))
	return NewDataResponse(frame)
}
