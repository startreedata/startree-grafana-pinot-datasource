package resources

import (
	"context"
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/dataquery"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/logger"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/templates"
	"net/http"
	"time"
)

type ResourceHandler struct {
	client *pinotlib.PinotClient
	router *mux.Router
}

type Response struct {
	Code  int    `json:"code"`
	Error string `json:"error,omitempty"`

	*GetDatabasesResponse
	*GetTablesResponse
	*GetTableSchemaResponse
	*DistinctValuesResponse
	*SqlPreviewResponse
}

type GetDatabasesResponse struct {
	Databases []string `json:"databases"`
}

type GetTablesResponse struct {
	Tables []string `json:"tables"`
}

type GetTableSchemaResponse struct {
	Schema pinotlib.TableSchema `json:"schema"`
}

type SqlPreviewResponse struct {
	Sql string `json:"sql"`
}

type DistinctValuesResponse struct {
	ValueExprs []string `json:"valueExprs"`
}

func NewPinotResourceHandler(client *pinotlib.PinotClient) *ResourceHandler {
	router := mux.NewRouter()

	handler := ResourceHandler{client: client, router: router}

	router.HandleFunc("/databases", adaptHandler(handler.GetDatabases))
	router.HandleFunc("/tables/{table}/schema", adaptHandler(handler.GetTableSchema))
	router.HandleFunc("/tables", adaptHandler(handler.GetTables))
	router.HandleFunc("/preview", adaptHandlerWithBody(handler.SqlBuilderPreview))
	router.HandleFunc("/codePreview", adaptHandlerWithBody(handler.SqlCodePreview))
	router.HandleFunc("/distinctValues", adaptHandlerWithBody(handler.DistinctValues))
	router.HandleFunc("/distinctValuesSqlPreview", adaptHandlerWithBody(handler.DistinctValuesSqlPreview))

	return &handler
}

func (x *ResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	x.router.ServeHTTP(w, r)
}

func (x *ResourceHandler) GetDatabases(r *http.Request) *Response {
	databases, err := x.client.ListDatabases(r.Context())

	if pinotlib.IsControllerStatusError(err, http.StatusForbidden) {
		logger.Logger.Error("pinotClient.ListDatabases() failed:", err.Error())
		return newEmptyResponse(http.StatusOK)
	} else if err != nil {
		return newInternalServerErrorResponse(err)
	}
	return &Response{Code: http.StatusOK, GetDatabasesResponse: &GetDatabasesResponse{Databases: databases}}
}

func (x *ResourceHandler) GetTables(r *http.Request) *Response {
	tables, err := x.client.ListTables(r.Context())
	if err != nil {
		return newInternalServerErrorResponse(err)
	}
	return &Response{Code: http.StatusOK, GetTablesResponse: &GetTablesResponse{Tables: tables}}
}

func (x *ResourceHandler) GetTableSchema(r *http.Request) *Response {
	vars := mux.Vars(r)
	table := vars["table"]

	schema, err := x.client.GetTableSchema(r.Context(), table)
	if err != nil {
		return newInternalServerErrorResponse(err)
	}
	return &Response{Code: http.StatusOK, GetTableSchemaResponse: &GetTableSchemaResponse{Schema: schema}}
}

type SqlBuilderPreviewRequest struct {
	TimeRange           dataquery.TimeRange         `json:"timeRange"`
	IntervalSize        string                      `json:"intervalSize"`
	DatabaseName        string                      `json:"databaseName"`
	TableName           string                      `json:"tableName"`
	TimeColumn          string                      `json:"timeColumn"`
	MetricColumn        string                      `json:"metricColumn"`
	GroupByColumns      []string                    `json:"groupByColumns"`
	AggregationFunction string                      `json:"aggregationFunction"`
	DimensionFilters    []dataquery.DimensionFilter `json:"filters"`
	Limit               int64                       `json:"limit"`
	Granularity         string                      `json:"granularity"`
	OrderByClauses      []dataquery.OrderByClause   `json:"orderBy"`
	QueryOptions        []dataquery.QueryOption     `json:"queryOptions"`
	ExpandMacros        bool                        `json:"expandMacros"`
}

func (x *ResourceHandler) SqlBuilderPreview(ctx context.Context, data SqlBuilderPreviewRequest) *Response {
	if data.TableName == "" {
		return newEmptyResponse(http.StatusOK)
	}

	tableSchema, err := x.client.GetTableSchema(ctx, data.TableName)
	if err != nil {
		// No need to surface this error in Grafana.
		logger.Logger.Error("pinotClient.GetTableSchema() failed:", err.Error())
		return newEmptyResponse(http.StatusOK)
	}

	driver, err := dataquery.NewPinotQlBuilderDriver(dataquery.PinotQlBuilderParams{
		TableSchema:         tableSchema,
		TimeRange:           data.TimeRange,
		IntervalSize:        parseIntervalSize(data.IntervalSize),
		DatabaseName:        data.DatabaseName,
		TableName:           data.TableName,
		TimeColumn:          data.TimeColumn,
		MetricColumn:        data.MetricColumn,
		GroupByColumns:      data.GroupByColumns,
		AggregationFunction: data.AggregationFunction,
		DimensionFilters:    data.DimensionFilters,
		Limit:               data.Limit,
		Granularity:         data.Granularity,
		OrderByClauses:      data.OrderByClauses,
		QueryOptions:        data.QueryOptions,
	})
	if err != nil {
		logger.Logger.Error("newPinotDriver() failed:", err.Error())
		// No need to surface this error in Grafana.
		return newEmptyResponse(http.StatusOK)
	}

	sql, err := driver.RenderPinotSql(data.ExpandMacros)
	if err != nil {
		logger.Logger.Error("pinotDriver.RenderSql() failed:", err.Error())
		// No need to surface this error in Grafana.
		return newEmptyResponse(http.StatusOK)
	}

	return newSqlPreviewResponse(sql)
}

type SqlCodePreviewRequest struct {
	TimeRange         dataquery.TimeRange `json:"timeRange"`
	IntervalSize      string              `json:"intervalSize"`
	TableName         string              `json:"tableName"`
	TimeColumnAlias   string              `json:"timeColumnAlias"`
	TimeColumnFormat  string              `json:"timeColumnFormat"`
	MetricColumnAlias string              `json:"metricColumnAlias"`
	Code              string              `json:"code"`
}

func (x *ResourceHandler) SqlCodePreview(ctx context.Context, data SqlCodePreviewRequest) *Response {
	if data.TableName == "" {
		logger.Logger.Info("received code preview request without table selection.")
		return newEmptyResponse(http.StatusOK)
	}

	tableSchema, err := x.client.GetTableSchema(ctx, data.TableName)
	if err != nil {
		logger.Logger.Error("pinotClient.GetTableSchema() failed:", err.Error())
		// No need to surface this error in Grafana.
		return newEmptyResponse(http.StatusOK)
	}

	driver, err := dataquery.NewPinotQlCodeDriver(dataquery.PinotQlCodeDriverParams{
		TableName:         data.TableName,
		TimeRange:         data.TimeRange,
		IntervalSize:      parseIntervalSize(data.IntervalSize),
		TableSchema:       tableSchema,
		TimeColumnAlias:   data.TimeColumnAlias,
		TimeColumnFormat:  data.TimeColumnFormat,
		MetricColumnAlias: data.MetricColumnAlias,
		Code:              data.Code,
	})
	if err != nil {
		logger.Logger.Error("NewPinotQlCodeDriver() failed:", err.Error())
		// No need to surface this error in Grafana.
		return newEmptyResponse(http.StatusOK)
	}

	sql, err := driver.RenderPinotSql()
	if err != nil {
		logger.Logger.Error("RenderPinotSql() failed:", err.Error())
		// No need to surface this error in Grafana.
		return newEmptyResponse(http.StatusOK)
	}

	return newSqlPreviewResponse(sql)
}

type DistinctValuesRequest struct {
	TableName        string                      `json:"tableName"`
	ColumnName       string                      `json:"columnName"`
	TimeRange        *dataquery.TimeRange        `json:"timeRange"`
	TimeColumn       string                      `json:"timeColumn"`
	DimensionFilters []dataquery.DimensionFilter `json:"filters"`
}

func (x *ResourceHandler) DistinctValues(ctx context.Context, data DistinctValuesRequest) *Response {
	sql, err := x.getDistinctValuesSql(ctx, data)
	if err != nil {
		return newInternalServerErrorResponse(err)
	}
	if sql == "" {
		return &Response{Code: http.StatusOK, DistinctValuesResponse: &DistinctValuesResponse{}}
	}

	results, err := x.client.ExecuteSQL(ctx, data.TableName, sql)
	if err != nil {
		return newInternalServerErrorResponse(err)
	}

	return &Response{Code: http.StatusOK, DistinctValuesResponse: &DistinctValuesResponse{
		ValueExprs: dataquery.ExtractColumnExpr(results.ResultTable, 0),
	}}
}

func (x *ResourceHandler) DistinctValuesSqlPreview(ctx context.Context, data DistinctValuesRequest) *Response {
	sql, err := x.getDistinctValuesSql(ctx, data)
	if err != nil {
		return newErrorResponse(http.StatusInternalServerError, err)
	}

	return newSqlPreviewResponse(sql)
}

func (x *ResourceHandler) getDistinctValuesSql(ctx context.Context, data DistinctValuesRequest) (string, error) {
	if data.TableName == "" || data.ColumnName == "" {
		return "", nil
	}

	var timeFilterExpr string
	if data.TimeRange != nil {
		tableSchema, err := x.client.GetTableSchema(ctx, data.TableName)
		if err != nil {
			return "", err
		}

		exprBuilder, err := dataquery.TimeExpressionBuilderFor(tableSchema, data.TimeColumn)
		if err != nil {
			return "", err
		}

		timeFilterExpr = exprBuilder.TimeFilterExpr(*data.TimeRange)
	}

	return templates.RenderDistinctValuesSql(templates.DistinctValuesSqlParams{
		ColumnName:           data.ColumnName,
		TableName:            data.TableName,
		TimeFilterExpr:       timeFilterExpr,
		DimensionFilterExprs: dataquery.FilterExprsFrom(data.DimensionFilters),
	})
}

func newSqlPreviewResponse(sql string) *Response {
	return &Response{Code: http.StatusOK, SqlPreviewResponse: &SqlPreviewResponse{Sql: sql}}
}

func newEmptyResponse(code int) *Response {
	return &Response{Code: code}
}

func newBadRequestResponse(err error) *Response {
	return newErrorResponse(http.StatusBadRequest, err)
}

func newInternalServerErrorResponse(err error) *Response {
	return newErrorResponse(http.StatusInternalServerError, err)
}

func newErrorResponse(code int, err error) *Response {
	return &Response{Code: code, Error: err.Error()}
}

func adaptHandler(handler func(r *http.Request) *Response) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeResponse(w, handler(r))
	}
}

func adaptHandlerWithBody[A any](handler func(ctx context.Context, data A) *Response) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var data A
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			writeResponse(w, newBadRequestResponse(err))
			return
		}
		writeResponse(w, handler(r.Context(), data))
	}
}

func parseIntervalSize(intervalSize string) time.Duration {
	if intervalSize == "1d" {
		return time.Second * 24 * 3600
	}
	interval, _ := time.ParseDuration(intervalSize)
	return interval
}

func writeResponse(w http.ResponseWriter, resp *Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.Code)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logger.Logger.Error("failed to write http response: ", err)
	}
}
