package main

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"os"
)

var PluginId = "grafana-pinot-datasource"

func main() {
	if err := datasource.Manage(PluginId, plugin.NewInstance, datasource.ManageOpts{}); err != nil {
		log.WithError(err).Error("Failed to serve datasource.")
		os.Exit(1)
	}
}
