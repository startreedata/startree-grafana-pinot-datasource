package main

import (
	"encoding/json"
	"os"
)

const PinotDataSourceType = "startree-pinot-datasource"

// Converts a Grafana dashboard JSON to use Pinot as the datasource.
// Usage: cat my_dashboard.json | PINOT_TABLE=mytable PINOT_DATASOURCE_UID=myuid go run cmd/migratedash/main.go > my_dashboard_pinot.json

func main() {
	pinotTable := os.Getenv("PINOT_TABLE")
	if pinotTable == "" {
		panic("Please set PINOT_TABLE to the name of the Pinot table")
	}

	dataSourceUid := os.Getenv("PINOT_DATASOURCE_UID")
	if dataSourceUid == "" {
		panic("Please set PINOT_DATASOURCE_UID to the UID of the Pinot datasource")
	}

	var pinotDatasource = map[string]interface{}{
		"type": PinotDataSourceType,
		"uid":  dataSourceUid,
	}

	var dashboard map[string]interface{}
	if err := json.NewDecoder(os.Stdin).Decode(&dashboard); err != nil {
		panic(err)
	}

	if _, ok := dashboard["panels"]; !ok {
		panic("panels not found")
	}

	panels := dashboard["panels"].([]interface{})
	for i := range panels {
		panel := panels[i].(map[string]interface{})
		if panel["datasource"] != nil && panel["datasource"].(map[string]interface{})["type"] == "prometheus" {
			panel["datasource"] = pinotDatasource
		}
		if panel["targets"] == nil {
			panels[i] = panel
			continue
		}

		targets := panel["targets"].([]interface{})
		for j := range targets {
			target := targets[j].(map[string]interface{})
			if target["datasource"] == nil || target["datasource"].(map[string]interface{})["type"] != "prometheus" {
				continue
			}
			targets[j] = map[string]interface{}{
				"refId":      target["refId"],
				"datasource": pinotDatasource,
				"promQlCode": target["expr"],
				"queryType":  "PromQL",
				"tableName":  pinotTable,
			}
		}
		panel["targets"] = targets
		panels[i] = panel
	}

	if err := json.NewEncoder(os.Stdout).Encode(dashboard); err != nil {
		panic(err)
	}
}
