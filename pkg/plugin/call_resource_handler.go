package plugin

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/resource/httpadapter"
	"net/http"
)

// ref: https://community.grafana.com/t/how-to-add-a-resource-handler-for-your-data-source/58924

func NewCallResourceHandler(client *PinotClient) backend.CallResourceHandler {
	router := mux.NewRouter()
	router.HandleFunc("/tables/{tableName}/schema", newTableSchemaHandler(client))
	router.HandleFunc("/tables", newTablesHandler(client))
	router.HandleFunc("/tables/{tableName}/{dimensionColumn}/distinctValues", newDimensionValuesHandler(client))
	return httpadapter.New(router)
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

func newTablesHandler(client *PinotClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tables, err := client.ListTables(r.Context())
		if err != nil {
			Logger.Error(err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		writeJsonData(w, GetTablesResponse{
			Tables: tables,
		})
	}
}

func newTableSchemaHandler(client *PinotClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		tableName := vars["tableName"]

		schema, err := client.GetTableSchema(r.Context(), tableName)
		if err != nil {
			Logger.Error(err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		writeJsonData(w, GetTableSchemaResponse{
			Schema: schema,
		})
	}
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
