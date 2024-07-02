package plugin

import (
	"github.com/gorilla/mux"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/resource/httpadapter"
)

// ref: https://community.grafana.com/t/how-to-add-a-resource-handler-for-your-data-source/58924

func NewCallResourceHandler(client *PinotClient) backend.CallResourceHandler {
	router := mux.NewRouter()

	handler := PinotResourceHandler{client: client}
	router.HandleFunc("/databases", handler.getDatabases)
	router.HandleFunc("/tables/{table}/schema", handler.getTableSchema)
	router.HandleFunc("/tables", handler.getTables)
	router.HandleFunc("/preview", handler.SqlPreview)
	router.HandleFunc("/distinctValues", handler.DistinctValues)
	return httpadapter.New(router)
}
