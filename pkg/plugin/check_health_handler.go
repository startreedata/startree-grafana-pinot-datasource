package plugin

import (
	"context"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
)

func NewCheckHealthHandler(client *pinotlib.PinotClient) backend.CheckHealthHandler {
	return backend.CheckHealthHandlerFunc(func(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Test connection to controller
		if tables, err := client.ListTables(ctx); err != nil {
			return &backend.CheckHealthResult{
				Status:  backend.HealthStatusError,
				Message: err.Error(),
			}, nil
		} else if len(tables) == 0 {
			return &backend.CheckHealthResult{
				Status:  backend.HealthStatusError,
				Message: fmt.Sprintf("Got an empty list of tables from %s. Please check the authentication and database settings.", client.Properties().ControllerUrl),
			}, nil
		}

		// Test connection to broker
		if _, err := client.ExecuteSqlQuery(ctx, pinotlib.NewSqlQuery("SELECT 1")); err != nil {
			return &backend.CheckHealthResult{
				Status:  backend.HealthStatusError,
				Message: err.Error(),
			}, nil
		}

		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusOk,
			Message: "Pinot data source is working",
		}, nil
	})
}
