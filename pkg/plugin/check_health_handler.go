package plugin

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
)

func NewCheckHealthHandler(client *pinotlib.PinotClient) backend.CheckHealthHandler {
	return backend.CheckHealthHandlerFunc(func(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Test connection to controller
		if _, err := client.ListTables(ctx); err != nil {
			return &backend.CheckHealthResult{
				Status:  backend.HealthStatusError,
				Message: err.Error(),
			}, nil
		}

		// Test connection to broker
		if _, err := client.ExecuteSQL(ctx, "", "select 1"); err != nil {
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
