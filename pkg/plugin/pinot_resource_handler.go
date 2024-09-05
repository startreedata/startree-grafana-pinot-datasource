package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gorilla/mux"
	"github.com/startree/pinot/pkg/plugin/templates"
	"net/http"
	"strings"
	"time"
)

type PinotResourceHandler struct {
	client *PinotClient
	router *mux.Router
}

func NewPinotResourceHandler(client *PinotClient) *PinotResourceHandler {
	router := mux.NewRouter()

	handler := PinotResourceHandler{client: client, router: router}

	router.HandleFunc("/databases", handler.GetDatabases)
	router.HandleFunc("/tables/{table}/schema", handler.GetTableSchema)
	router.HandleFunc("/tables", handler.GetTables)
	router.HandleFunc("/preview", adaptHandler(handler.SqlBuilderPreview))
	router.HandleFunc("/codePreview", adaptHandler(handler.SqlCodePreview))
	router.HandleFunc("/distinctValues", adaptHandler(handler.DistinctValues))
	router.HandleFunc("/distinctValuesSqlPreview", adaptHandler(handler.DistinctValuesSqlPreview))

	return &handler
}

func (x *PinotResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	x.router.ServeHTTP(w, r)
}

type GetDatabasesResponse struct {
	Databases []string `json:"databases"`
}

func (x *PinotResourceHandler) GetDatabases(w http.ResponseWriter, r *http.Request) {
	databases, err := x.client.ListDatabases(r.Context())

	if err != nil {
		var controllerError *ControllerStatusError
		if errors.As(err, &controllerError) {
			if controllerError.StatusCode == http.StatusForbidden {
				writeJsonData(w, http.StatusOK, GetDatabasesResponse{Databases: []string{}})
				return
			}
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJsonData(w, http.StatusOK, GetDatabasesResponse{Databases: databases})
}

type GetTablesResponse struct {
	Tables []string `json:"tables"`
}

func (x *PinotResourceHandler) GetTables(w http.ResponseWriter, r *http.Request) {
	tables, err := x.client.ListTables(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJsonData(w, http.StatusOK, GetTablesResponse{Tables: tables})
}

type GetTableSchemaResponse struct {
	Schema TableSchema `json:"schema"`
}

func (x *PinotResourceHandler) GetTableSchema(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	table := vars["table"]

	schema, err := x.client.GetTableSchema(r.Context(), table)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJsonData(w, http.StatusOK, GetTableSchemaResponse{Schema: schema})
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
}

type SqlBuilderPreviewResponse struct {
	Sql string `json:"sql"`
}

func parseIntervalSize(intervalSize string) time.Duration {
	if intervalSize == "1d" {
		return time.Second * 24 * 3600
	}
	interval, _ := time.ParseDuration(intervalSize)
	return interval
}

func (x *PinotResourceHandler) SqlBuilderPreview(ctx context.Context, data SqlBuilderPreviewRequest) (int, *SqlBuilderPreviewResponse, error) {
	if data.TableName == "" {
		// Nothing to do.
		return http.StatusOK, &SqlBuilderPreviewResponse{}, nil
	}

	tableSchema, err := x.client.GetTableSchema(ctx, data.TableName)
	if err != nil {
		// No need to surface this error in Grafana.
		return http.StatusOK, nil, err
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
		// No need to surface this error in Grafana.
		return http.StatusOK, nil, err
	}

	sql, err := driver.RenderPinotSql()
	if err != nil {
		// No need to surface this error in Grafana.
		return http.StatusOK, nil, err
	}

	return http.StatusOK, &SqlBuilderPreviewResponse{Sql: sql}, nil
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

type SqlCodePreviewResponse struct {
	Sql string `json:"sql"`
}

func (x *PinotResourceHandler) SqlCodePreview(ctx context.Context, data SqlCodePreviewRequest) (int, *SqlCodePreviewResponse, error) {
	if data.TableName == "" {
		// Nothing to do.
		return http.StatusOK, &SqlCodePreviewResponse{}, nil
	}

	tableSchema, err := x.client.GetTableSchema(ctx, data.TableName)
	if err != nil {
		// No need to surface this error in Grafana.
		return http.StatusOK, nil, err
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
		// No need to surface this error in Grafana.
		return http.StatusOK, nil, err
	}

	sql, err := driver.RenderPinotSql()
	if err != nil {
		// No need to surface this error in Grafana.
		return http.StatusOK, nil, err
	}

	return http.StatusOK, &SqlCodePreviewResponse{
		Sql: strings.TrimSpace(sql),
	}, nil
}

type DistinctValuesRequest struct {
	TableName        string            `json:"tableName"`
	ColumnName       string            `json:"columnName"`
	TimeRange        *TimeRange        `json:"timeRange"`
	TimeColumn       string            `json:"timeColumn"`
	DimensionFilters []DimensionFilter `json:"filters"`
}

type DistinctValuesResponse struct {
	ValueExprs []string `json:"valueExprs"`
}

func (x *PinotResourceHandler) DistinctValues(ctx context.Context, data DistinctValuesRequest) (int, *DistinctValuesResponse, error) {
	sql, err := x.getDistinctValuesSql(ctx, data)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}
	if sql == "" {
		return http.StatusOK, &DistinctValuesResponse{}, nil
	}

	results, err := x.client.ExecuteSQL(ctx, data.TableName, sql)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	return http.StatusOK, &DistinctValuesResponse{
		ValueExprs: ExtractColumnExpr(results.ResultTable, 0),
	}, nil
}

type DistinctValuesSqlPreviewResponse struct {
	Sql string `json:"sql"`
}

func (x *PinotResourceHandler) DistinctValuesSqlPreview(ctx context.Context, data DistinctValuesRequest) (int, *DistinctValuesSqlPreviewResponse, error) {
	sql, err := x.getDistinctValuesSql(ctx, data)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}
	return http.StatusOK, &DistinctValuesSqlPreviewResponse{Sql: sql}, nil
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

func adaptHandler[A any, B any](handler func(ctx context.Context, data A) (int, B, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var data A
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		code, respData, err := handler(r.Context(), data)
		if err != nil {
			writeError(w, code, err)
			return
		}
		writeJsonData(w, code, respData)
	}
}

type ErrorResponse struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, code int, err error) {
	Logger.Error(err.Error())
	writeJsonData(w, code, ErrorResponse{Error: err.Error()})
}

func writeJsonData(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		Logger.Error("failed to write http response: ", err)
	}
}
