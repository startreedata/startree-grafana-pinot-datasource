package main

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin"
	"os"
)

func main() {
	if err := datasource.Manage("startree-pinot-datasource", plugin.NewDatasource, datasource.ManageOpts{}); err != nil {
		log.DefaultLogger.Error(err.Error())
		os.Exit(1)
	}
}
