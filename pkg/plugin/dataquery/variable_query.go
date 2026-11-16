package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"strings"
	"time"
)

var _ ExecutableQuery = VariableQuery{}

type VariableQuery struct {
	VariableType VariableQueryType
	TableName    string
	ColumnName   string
	ColumnType   ColumnType
	PinotQlCode  string
	// TimeRange is the dashboard time range; only applied when the variable refreshes on time range change.
	TimeRange    TimeRange
	IntervalSize time.Duration
}

func (query VariableQuery) hasTimeRange() bool {
	return !query.TimeRange.From.IsZero() && !query.TimeRange.To.IsZero()
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

	engine := MacroEngine{TableName: query.TableName, TimeRange: query.TimeRange, IntervalSize: query.IntervalSize}
	if engine.IntervalSize <= 0 {
		engine.IntervalSize = time.Minute
	}
	if query.TableName != "" {
		schema, err := client.GetTableSchema(ctx, query.TableName)
		if err != nil {
			return NewPluginErrorResponse(err)
		}
		configs, err := client.ListTableConfigs(ctx, query.TableName)
		if err != nil {
			return NewPluginErrorResponse(err)
		}
		engine.TableSchema, engine.TableConfigs = schema, configs
	}
	// Variables previously only expanded $__table, so time macros reached Pinot unexpanded and failed.
	sqlCode, err := engine.ExpandMacros(ctx, sqlCode)
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

	timeFilterExpr, err := query.distinctValuesTimeFilter(ctx, client)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	sql, err := pinot.RenderDistinctValuesSql(pinot.DistinctValuesSqlParams{
		ColumnExpr:     pinot.ObjectExpr(query.ColumnName),
		TableName:      query.TableName,
		TimeFilterExpr: timeFilterExpr,
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

// distinctValuesTimeFilter limits distinct values to the dashboard time range on the table's primary
// time column. Tables without a resolvable time column keep the previous unfiltered behavior.
func (query VariableQuery) distinctValuesTimeFilter(ctx context.Context, client *pinot.Client) (pinot.SqlExpr, error) {
	if !query.hasTimeRange() {
		return "", nil
	}
	schema, err := client.GetTableSchema(ctx, query.TableName)
	if err != nil {
		return "", err
	}
	configs, err := client.ListTableConfigs(ctx, query.TableName)
	if err != nil {
		return "", err
	}
	timeColumn, err := pinot.DefaultTimeColumn(schema, configs)
	if err != nil {
		return "", nil
	}
	format, err := pinot.GetTimeColumnFormat(schema, timeColumn)
	if err != nil {
		return "", nil
	}
	return pinot.TimeFilterExpr(pinot.TimeFilter{
		Column: timeColumn,
		Format: format,
		From:   query.TimeRange.From,
		To:     query.TimeRange.To,
	}), nil
}
