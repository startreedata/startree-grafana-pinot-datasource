package plugin

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/startree/pinot/pkg/plugin/templates"
	"net/http"
	"strings"
	"time"
)

type PinotResourceHandler struct {
	client *PinotClient
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type GetDatabasesResponse struct {
	Databases []string `json:"databases"`
}

func (x *PinotResourceHandler) getDatabases(w http.ResponseWriter, r *http.Request) {
	databases, err := x.client.ListDatabases(r.Context())
	if err != nil {
		x.writeError(w, http.StatusInternalServerError, err)
		return
	}

	x.writeJsonData(w, GetDatabasesResponse{Databases: databases})
}

type GetTablesResponse struct {
	Tables []string `json:"tables"`
}

func (x *PinotResourceHandler) getTables(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	database := params.Get("database")

	tables, err := x.client.ListTables(r.Context(), database)
	if err != nil {
		x.writeError(w, http.StatusInternalServerError, err)
		return
	}

	x.writeJsonData(w, GetTablesResponse{Tables: tables})
}

type GetTableSchemaResponse struct {
	Schema TableSchema `json:"schema"`
}

func (x *PinotResourceHandler) getTableSchema(w http.ResponseWriter, r *http.Request) {
	database := x.databaseFrom(r)

	vars := mux.Vars(r)
	table := vars["table"]

	schema, err := x.client.GetTableSchema(r.Context(), database, table)
	if err != nil {
		x.writeError(w, http.StatusInternalServerError, err)
		return
	}

	x.writeJsonData(w, GetTableSchemaResponse{Schema: schema})
}

type SqlPreviewRequest struct {
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
}

type SqlPreviewResponse struct {
	Sql string `json:"sql"`
}

func (x *PinotResourceHandler) SqlPreview(w http.ResponseWriter, r *http.Request) {
	var data SqlPreviewRequest
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		x.writeError(w, http.StatusBadRequest, err)
		return
	}

	if data.TableName == "" {
		// Nothing to do.
		x.writeJsonData(w, &SqlPreviewResponse{})
		return
	}

	tableSchema, err := x.client.GetTableSchema(r.Context(), data.DatabaseName, data.TableName)
	if err != nil {
		x.writeError(w, http.StatusInternalServerError, err)
		return
	}

	var interval time.Duration
	if data.IntervalSize == "1d" {
		interval = time.Second * 24 * 3600
	} else {
		interval, _ = time.ParseDuration(data.IntervalSize)
	}

	driver, err := NewPinotQlBuilderDriver(PinotQlBuilderParams{
		TableSchema:         tableSchema,
		TimeRange:           data.TimeRange,
		IntervalSize:        interval,
		DatabaseName:        data.DatabaseName,
		TableName:           data.TableName,
		TimeColumn:          data.TimeColumn,
		MetricColumn:        data.MetricColumn,
		GroupByColumns:      data.GroupByColumns,
		AggregationFunction: data.AggregationFunction,
		DimensionFilters:    data.DimensionFilters,
		Limit:               data.Limit,
		Granularity:         data.Granularity,
	})

	if err != nil {
		x.writeError(w, http.StatusInternalServerError, err)
		return
	}

	sql, err := driver.RenderPinotSql()
	if err != nil {
		x.writeError(w, http.StatusInternalServerError, err)
		return
	}

	x.writeJsonData(w, &SqlPreviewResponse{Sql: strings.TrimSpace(sql)})
}

type DistinctValuesRequest struct {
	TimeRange        TimeRange         `json:"timeRange"`
	DatabaseName     string            `json:"databaseName"`
	TableName        string            `json:"tableName"`
	ColumnName       string            `json:"columnName"`
	TimeColumn       string            `json:"timeColumn"`
	DimensionFilters []DimensionFilter `json:"filters"`
}

type DistinctValuesResponse struct {
	ValueExprs []string `json:"valueExprs"`
}

func (x *PinotResourceHandler) DistinctValues(w http.ResponseWriter, r *http.Request) {
	var data DistinctValuesRequest
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		x.writeError(w, http.StatusBadRequest, err)
		return
	}

	if data.TableName == "" || data.ColumnName == "" {
		// Nothing to do.
		x.writeJsonData(w, &DistinctValuesResponse{})
		return
	}

	tableSchema, err := x.client.GetTableSchema(r.Context(), data.DatabaseName, data.TableName)
	if err != nil {
		x.writeError(w, http.StatusInternalServerError, err)
		return
	}

	exprBuilder, err := TimeExpressionBuilderFor(tableSchema, data.TimeColumn)
	if err != nil {
		// TODO: Handle this error
		x.writeError(w, http.StatusInternalServerError, err)
		return
	}

	sql, err := templates.RenderDistinctValuesSql(templates.DistinctValuesSqlParams{
		ColumnName:           data.ColumnName,
		TableName:            data.TableName,
		TimeFilterExpr:       exprBuilder.TimeFilterExpr(data.TimeRange),
		DimensionFilterExprs: FilterExprsFrom(data.DimensionFilters),
	})
	if err != nil {
		x.writeError(w, http.StatusInternalServerError, err)
		return
	}

	results, err := x.client.ExecuteSQL(r.Context(), data.TableName, sql)
	if err != nil {
		x.writeError(w, http.StatusInternalServerError, err)
		return
	}

	x.writeJsonData(w, &DistinctValuesResponse{
		ValueExprs: ExtractColumnExpr(results.ResultTable, 0),
	})
}

func (x *PinotResourceHandler) databaseFrom(r *http.Request) string {
	params := r.URL.Query()
	return params.Get("database")
}

func (x *PinotResourceHandler) writeError(w http.ResponseWriter, code int, err error) {
	Logger.Error(err.Error())
	w.WriteHeader(code)
	x.writeJsonData(w, ErrorResponse{Error: err.Error()})
}

func (x *PinotResourceHandler) writeJsonData(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		Logger.Error("failed to write http response: ", err)
	}
}
