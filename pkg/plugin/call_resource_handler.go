package plugin

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/resource/httpadapter"
	"net/http"
	"time"
)

// ref: https://community.grafana.com/t/how-to-add-a-resource-handler-for-your-data-source/58924

func NewCallResourceHandler(client *PinotClient) backend.CallResourceHandler {
	router := mux.NewRouter()

	handler := PinotResourceHandler{client: client}
	router.HandleFunc("/databases", handler.getDatabases)
	router.HandleFunc("/tables/{table}/schema", handler.getTableSchema)
	router.HandleFunc("/tables", handler.getTables)
	router.HandleFunc("/preview", handler.getPreview)
	return httpadapter.New(router)
}

type PinotResourceHandler struct {
	client *PinotClient
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

type GetTableDimensionDistinctValuesResponse struct {
	Values []string `json:"values"`
}

func (x PinotResourceHandler) getDatabases(w http.ResponseWriter, r *http.Request) {
	databases, err := x.client.ListDatabases(r.Context())
	if err != nil {
		Logger.Error(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	writeJsonData(w, GetDatabasesResponse{
		Databases: databases,
	})
}

func (x PinotResourceHandler) getTables(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	database := params.Get("database")

	tables, err := x.client.ListTables(r.Context(), database)
	if err != nil {
		Logger.Error(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	writeJsonData(w, GetTablesResponse{
		Tables: tables,
	})
}

func (x PinotResourceHandler) getTableSchema(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	database := params.Get("database")

	vars := mux.Vars(r)
	table := vars["table"]

	schema, err := x.client.GetTableSchema(r.Context(), database, table)
	if err != nil {
		Logger.Error(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	writeJsonData(w, GetTableSchemaResponse{
		Schema: schema,
	})
}

type GetSqlPreviewRequest struct {
	TimeRange           TimeRange     `json:"timeRange"`
	IntervalSize        time.Duration `json:"intervalSize"`
	DatabaseName        string        `json:"databaseName"`
	TableName           string        `json:"tableName"`
	TimeColumn          string        `json:"timeColumn"`
	MetricColumn        string        `json:"metricColumn"`
	DimensionColumns    []string      `json:"dimensionColumns"`
	AggregationFunction string        `json:"aggregationFunction"`
}

type GetSqlPreviewResponse struct {
	Sql string `json:"sql"`
}

func (x PinotResourceHandler) getPreview(w http.ResponseWriter, r *http.Request) {
	var data GetSqlPreviewRequest
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		Logger.Error(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if data.TableName == "" {
		// Nothing to do.
		writeJsonData(w, &GetSqlPreviewResponse{})
		return
	}

	tableSchema, err := x.client.GetTableSchema(r.Context(), data.DatabaseName, data.TableName)
	if err != nil {
		Logger.Error(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	driver, err := NewTimeSeriesDriver(TimeSeriesDriverParams{
		TableSchema:         tableSchema,
		TimeRange:           data.TimeRange,
		IntervalSize:        data.IntervalSize,
		DatabaseName:        data.DatabaseName,
		TableName:           data.TableName,
		TimeColumn:          data.TimeColumn,
		MetricColumn:        data.MetricColumn,
		DimensionColumns:    data.DimensionColumns,
		AggregationFunction: data.AggregationFunction,
	})

	if err != nil {
		// TODO: In most cases, this indicates that the params aren't valid, but dont need to source this error to ui.
		//  How else could I handle this error? Maybe an incomplete message or something?
		Logger.Error(err.Error())
		writeJsonData(w, &GetSqlPreviewResponse{})
		return
	}

	sql, err := driver.RenderPinotSql()
	if err != nil {
		Logger.Error(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	writeJsonData(w, &GetSqlPreviewResponse{Sql: sql})
}

func newDimensionValuesHandler(client *PinotClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		tableName := vars["tableName"]
		dimensionColumn := vars["dimensionColumn"]

		query := fmt.Sprintf(`SELECT DISTINCT "%s" FROM "%s"`, dimensionColumn, tableName)
		resp, err := client.ExecuteSQL(r.Context(), tableName, query)

		if err != nil {
			Logger.Error(err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		distinctValues := ExtractColumnExpr(resp.ResultTable, 0)
		writeJsonData(w, GetTableDimensionDistinctValuesResponse{distinctValues})
	}
}

func writeJsonData(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		Logger.Error("failed to write http response: ", err)
	}
}
