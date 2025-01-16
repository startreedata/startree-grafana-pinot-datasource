package plugin

import (
	"context"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/resource/httpadapter"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/dataquery"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/datasource"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/resources"
)

type disposerFunc func()

func (f disposerFunc) Dispose() { f() }

func NewInstance(_ context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	var config datasource.Config
	if err := config.ReadFrom(settings); err != nil {
		return nil, err
	}

	client := datasource.PinotClientOf(config)
	return &datasource.Datasource{
		QueryDataHandler:    newQueryDataHandler(client),
		CallResourceHandler: newCallResourceHandler(client),
		CheckHealthHandler:  newCheckHealthHandler(client),
		InstanceDisposer:    disposerFunc(func() { client.Close() }),
	}, nil
}

func newQueryDataHandler(client *pinotlib.PinotClient) backend.QueryDataHandler {
	return backend.QueryDataHandlerFunc(func(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
		response := backend.NewQueryDataResponse()
		for _, query := range req.Queries {
			log.FromContext(ctx).Debug("received query", "contents", string(query.JSON))
			response.Responses[query.RefID] = dataquery.ExecuteQuery(ctx, client, query)
		}
		return response, nil
	})
}

func newCallResourceHandler(client *pinotlib.PinotClient) backend.CallResourceHandler {
	return httpadapter.New(resources.NewResourceHandler(client))
}

func newCheckHealthHandler(client *pinotlib.PinotClient) backend.CheckHealthHandler {
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
