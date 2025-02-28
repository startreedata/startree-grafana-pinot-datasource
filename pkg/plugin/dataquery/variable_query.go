package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"strings"
)

var _ ExecutableQuery = VariableQuery{}

type VariableQuery struct {
	VariableType VariableQueryType
	TableName    string
	ColumnName   string
	ColumnType   ColumnType
	PinotQlCode  string
}

func (query VariableQuery) Execute(client *pinot.Client, ctx context.Context) backend.DataResponse {
	switch query.VariableType {
	case VariableQueryTypeColumnList:
		return query.getColumnList(ctx, client)
	case VariableQueryTypeDistinctValues:
		return query.getDistinctValues(ctx, client)
	case VariableQueryTypePinotQlCode:
		return query.getSqlResults(ctx, client)
	default:
		return query.getTableList(ctx, client)
	}
}

func (query VariableQuery) getSqlResults(ctx context.Context, client *pinot.Client) backend.DataResponse {
	sqlCode := strings.TrimSpace(query.PinotQlCode)
	if sqlCode == "" {
		return NewEmptyDataResponse()
	}

	sqlCode, err := MacroEngine{TableName: query.TableName}.ExpandTableName(ctx, sqlCode)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, client, pinot.NewSqlQuery(sqlCode))
	if !ok {
		return backendResp
	}

	cols := make([][]string, results.ColumnCount())
	for colId := 0; colId < results.ColumnCount(); colId++ {
		col, err := pinot.ExtractColumnAsStrings(results, colId)
		if err != nil {
			return NewPluginErrorResponse(err)
		}
		cols[colId] = col
	}

	values := make([]string, results.RowCount()*results.ColumnCount())
	for colIdx, col := range cols {
		for rowIdx, val := range col {
			// Extract values in table order.
			values[rowIdx*results.ColumnCount()+colIdx] = val
		}
	}
	values = pinot.GetDistinctValues(values)
	frame := data.NewFrame("result", data.NewField("codeValues", nil, values))
	return NewSqlQueryDataResponse(frame, exceptions)
}

func (query VariableQuery) getDistinctValues(ctx context.Context, client *pinot.Client) backend.DataResponse {
	if query.TableName == "" || query.ColumnName == "" {
		return NewEmptyDataResponse()
	}

	sql, err := pinot.RenderDistinctValuesSql(pinot.DistinctValuesSqlParams{
		ColumnExpr: pinot.ObjectExpr(query.ColumnName),
		TableName:  query.TableName,
	})
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, client, pinot.NewSqlQuery(sql))
	if !ok {
		return backendResp
	}

	values, err := pinot.ExtractColumnAsStrings(results, 0)
	if err != nil {
		return NewPluginErrorResponse(err)
	}
	frame := data.NewFrame("result", data.NewField("distinctValues", nil, values))
	return NewSqlQueryDataResponse(frame, exceptions)
}

func (query VariableQuery) getColumnList(ctx context.Context, client *pinot.Client) backend.DataResponse {
	if query.TableName == "" {
		return NewEmptyDataResponse()
	}
	schema, err := client.GetTableSchema(ctx, query.TableName)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	var columns []string
	columnType := getOrFallback(query.ColumnType, ColumnTypeAll)
	if columnType == ColumnTypeAll || columnType == ColumnTypeDateTime {
		for _, spec := range schema.DateTimeFieldSpecs {
			columns = append(columns, spec.Name)
		}
	}
	if columnType == ColumnTypeAll || columnType == ColumnTypeMetric {
		for _, spec := range schema.MetricFieldSpecs {
			columns = append(columns, spec.Name)
		}
	}
	if columnType == ColumnTypeAll || columnType == ColumnTypeDimension {
		for _, spec := range schema.DimensionFieldSpecs {
			columns = append(columns, spec.Name)
		}
	}

	frame := data.NewFrame("result", data.NewField("columns", nil, columns))
	return NewOkDataResponse(frame)
}

func (query VariableQuery) getTableList(ctx context.Context, client *pinot.Client) backend.DataResponse {
	tables, err := client.ListTables(ctx)
	if err != nil {
		return NewPluginErrorResponse(err)
	}
	frame := data.NewFrame("result", data.NewField("tables", nil, tables))
	return NewOkDataResponse(frame)
}
