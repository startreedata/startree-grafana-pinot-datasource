package plugin

import (
	"context"
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/startree/pinot/pkg/plugin/templates"
	"net/http"
	"time"
)

type PinotResourceHandler struct {
	client *PinotClient
	router *mux.Router
}

type PinotResourceResponse struct {
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
	Schema TableSchema `json:"schema"`
}

type SqlPreviewResponse struct {
	Sql string `json:"sql"`
}

type DistinctValuesResponse struct {
	ValueExprs []string `json:"valueExprs"`
}

func NewPinotResourceHandler(client *PinotClient) *PinotResourceHandler {
	router := mux.NewRouter()

	handler := PinotResourceHandler{client: client, router: router}

	router.HandleFunc("/databases", adaptHandler(handler.GetDatabases))
	router.HandleFunc("/tables/{table}/schema", adaptHandler(handler.GetTableSchema))
	router.HandleFunc("/tables", adaptHandler(handler.GetTables))
	router.HandleFunc("/preview", adaptHandlerWithBody(handler.SqlBuilderPreview))
	router.HandleFunc("/codePreview", adaptHandlerWithBody(handler.SqlCodePreview))
	router.HandleFunc("/distinctValues", adaptHandlerWithBody(handler.DistinctValues))
	router.HandleFunc("/distinctValuesSqlPreview", adaptHandlerWithBody(handler.DistinctValuesSqlPreview))

	return &handler
}

func (x *PinotResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	x.router.ServeHTTP(w, r)
}

func (x *PinotResourceHandler) GetDatabases(r *http.Request) *PinotResourceResponse {
	databases, err := x.client.ListDatabases(r.Context())

	if ErrorIsControllerStatus(err, http.StatusForbidden) {
		Logger.Error("pinotClient.ListDatabases() failed:", err.Error())
		return newEmptyResponse(http.StatusOK)
	} else if err != nil {
		return newInternalServerErrorResponse(err)
	}
	return &PinotResourceResponse{Code: http.StatusOK, GetDatabasesResponse: &GetDatabasesResponse{Databases: databases}}
}

func (x *PinotResourceHandler) GetTables(r *http.Request) *PinotResourceResponse {
	tables, err := x.client.ListTables(r.Context())
	if err != nil {
		return newInternalServerErrorResponse(err)
	}
	return &PinotResourceResponse{Code: http.StatusOK, GetTablesResponse: &GetTablesResponse{Tables: tables}}
}

func (x *PinotResourceHandler) GetTableSchema(r *http.Request) *PinotResourceResponse {
	vars := mux.Vars(r)
	table := vars["table"]

	schema, err := x.client.GetTableSchema(r.Context(), table)
	if err != nil {
		return newInternalServerErrorResponse(err)
	}
	return &PinotResourceResponse{Code: http.StatusOK, GetTableSchemaResponse: &GetTableSchemaResponse{Schema: schema}}
}

type SqlBuilderPreviewRequest struct {
	TimeRange           TimeRange         `json:"timeRange"`
	IntervalSize        string            `json:"intervalSize"`
	DatabaseName        string            `json:"databaseName"`
	TableName           string            `json:"tableName"`
	TimeColumn          string            `json:"timeColumn"`
	MetricColumn        string            `json:"metricColumn"`
	GroupByColumns      []string          `json:"groupByColumns"`
	AggregationFunction string            `json:"aggregationFunction"`
	DimensionFilters    []DimensionFilter `json:"filters"`
	Limit               int64             `json:"limit"`
	Granularity         string            `json:"granularity"`
	OrderByClauses      []OrderByClause   `json:"orderBy"`
	QueryOptions        []QueryOption     `json:"queryOptions"`
	ExpandMacros        bool              `json:"expandMacros"`
}

func (x *PinotResourceHandler) SqlBuilderPreview(ctx context.Context, data SqlBuilderPreviewRequest) *PinotResourceResponse {
	if data.TableName == "" {
		return newEmptyResponse(http.StatusOK)
	}

	tableSchema, err := x.client.GetTableSchema(ctx, data.TableName)
	if err != nil {
		// No need to surface this error in Grafana.
		Logger.Error("pinotClient.GetTableSchema() failed:", err.Error())
		return newEmptyResponse(http.StatusOK)
	}

	driver, err := NewPinotQlBuilderDriver(PinotQlBuilderParams{
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
		Logger.Error("newPinotDriver() failed:", err.Error())
		// No need to surface this error in Grafana.
		return newEmptyResponse(http.StatusOK)
	}

	var sql string
	if data.ExpandMacros {
		sql, err = driver.RenderPinotSql()
	} else {
		sql, err = driver.RenderPinotSqlWithMacros()
	}

	if err != nil {
		Logger.Error("pinotDriver.RenderSql() failed:", err.Error())
		// No need to surface this error in Grafana.
		return newEmptyResponse(http.StatusOK)
	}

	return newSqlPreviewResponse(sql)
}

type SqlCodePreviewRequest struct {
	TimeRange         TimeRange `json:"timeRange"`
	IntervalSize      string    `json:"intervalSize"`
	TableName         string    `json:"tableName"`
	TimeColumnAlias   string    `json:"timeColumnAlias"`
	TimeColumnFormat  string    `json:"timeColumnFormat"`
	MetricColumnAlias string    `json:"metricColumnAlias"`
	Code              string    `json:"code"`
}

func (x *PinotResourceHandler) SqlCodePreview(ctx context.Context, data SqlCodePreviewRequest) *PinotResourceResponse {
	if data.TableName == "" {
		Logger.Info("received code preview request without table selection.")
		return newEmptyResponse(http.StatusOK)
	}

	tableSchema, err := x.client.GetTableSchema(ctx, data.TableName)
	if err != nil {
		Logger.Error("pinotClient.GetTableSchema() failed:", err.Error())
		// TODO: This doesn't make sense.
		// No need to surface this error in Grafana.
		return newErrorResponse(http.StatusOK, err)
	}

	driver, err := NewPinotQlCodeDriver(PinotQlCodeDriverParams{
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
		Logger.Error("NewPinotQlCodeDriver() failed:", err.Error())
		// No need to surface this error in Grafana.
		return newErrorResponse(http.StatusOK, err)
	}

	sql, err := driver.RenderPinotSql()
	if err != nil {
		Logger.Error("RenderPinotSql() failed:", err.Error())
		// No need to surface this error in Grafana.
		return newErrorResponse(http.StatusOK, err)

	}

	return newSqlPreviewResponse(sql)
}

type DistinctValuesRequest struct {
	TableName        string            `json:"tableName"`
	ColumnName       string            `json:"columnName"`
	TimeRange        *TimeRange        `json:"timeRange"`
	TimeColumn       string            `json:"timeColumn"`
	DimensionFilters []DimensionFilter `json:"filters"`
}

func (x *PinotResourceHandler) DistinctValues(ctx context.Context, data DistinctValuesRequest) *PinotResourceResponse {
	sql, err := x.getDistinctValuesSql(ctx, data)
	if err != nil {
		return newInternalServerErrorResponse(err)
	}
	if sql == "" {
		return &PinotResourceResponse{Code: http.StatusOK, DistinctValuesResponse: &DistinctValuesResponse{}}
	}

	results, err := x.client.ExecuteSQL(ctx, data.TableName, sql)
	if err != nil {
		return newInternalServerErrorResponse(err)
	}

	return &PinotResourceResponse{Code: http.StatusOK, DistinctValuesResponse: &DistinctValuesResponse{
		ValueExprs: ExtractColumnExpr(results.ResultTable, 0),
	}}
}

func (x *PinotResourceHandler) DistinctValuesSqlPreview(ctx context.Context, data DistinctValuesRequest) *PinotResourceResponse {
	sql, err := x.getDistinctValuesSql(ctx, data)
	if err != nil {
		return newErrorResponse(http.StatusInternalServerError, err)
	}

	return newSqlPreviewResponse(sql)
}

func (x *PinotResourceHandler) getDistinctValuesSql(ctx context.Context, data DistinctValuesRequest) (string, error) {
	if data.TableName == "" || data.ColumnName == "" {
		return "", nil
	}

	var timeFilterExpr string
	if data.TimeRange != nil {
		tableSchema, err := x.client.GetTableSchema(ctx, data.TableName)
		if err != nil {
			return "", err
		}

		exprBuilder, err := TimeExpressionBuilderFor(tableSchema, data.TimeColumn)
		if err != nil {
			return "", err
		}

		timeFilterExpr = exprBuilder.TimeFilterExpr(*data.TimeRange)
	}

	return templates.RenderDistinctValuesSql(templates.DistinctValuesSqlParams{
		ColumnName:           data.ColumnName,
		TableName:            data.TableName,
		TimeFilterExpr:       timeFilterExpr,
		DimensionFilterExprs: FilterExprsFrom(data.DimensionFilters),
	})
}

func newSqlPreviewResponse(sql string) *PinotResourceResponse {
	return &PinotResourceResponse{Code: http.StatusOK, SqlPreviewResponse: &SqlPreviewResponse{Sql: sql}}
}

func newEmptyResponse(code int) *PinotResourceResponse {
	return &PinotResourceResponse{Code: code}
}

func newBadRequestResponse(err error) *PinotResourceResponse {
	return newErrorResponse(http.StatusBadRequest, err)
}

func newInternalServerErrorResponse(err error) *PinotResourceResponse {
	return newErrorResponse(http.StatusInternalServerError, err)
}

func newErrorResponse(code int, err error) *PinotResourceResponse {
	return &PinotResourceResponse{Code: code, Error: err.Error()}
}

func adaptHandler(handler func(r *http.Request) *PinotResourceResponse) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeResponse(w, handler(r))
	}
}

func adaptHandlerWithBody[A any](handler func(ctx context.Context, data A) *PinotResourceResponse) http.HandlerFunc {
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

func writeResponse(w http.ResponseWriter, resp *PinotResourceResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.Code)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		Logger.Error("failed to write http response: ", err)
	}
}
