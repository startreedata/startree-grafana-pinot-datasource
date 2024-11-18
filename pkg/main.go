package main

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"os"
)

func main() {
	if err := datasource.Manage("startree-pinot-datasource", plugin.NewDatasource, datasource.ManageOpts{}); err != nil {
		log.WithError(err).Error("Failed to serve datasource.")
		os.Exit(1)
	}
}
