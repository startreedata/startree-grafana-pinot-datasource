package plugin

import (
	"context"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/resource/httpadapter"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/dataquery"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/resources"
)

var (
	_ instancemgmt.Instance         = (*Datasource)(nil)
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ backend.CallResourceHandler   = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

type Datasource struct {
	backend.QueryDataHandler
	backend.CallResourceHandler
	backend.CheckHealthHandler
	instancemgmt.InstanceDisposer
}

func NewInstance(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	var config Config
	if err := config.ReadFrom(settings); err != nil {
		return nil, err
	}

	opts, err := settings.HTTPClientOptions(ctx)
	if err != nil {
		return nil, fmt.Errorf("http client options: %w", err)
	}
	opts.ForwardHTTPHeaders = true

	httpClient, err := httpclient.New(opts)
	if err != nil {
		return nil, fmt.Errorf("httpclient new: %w", err)
	}

	client := PinotClientOf(httpClient, config)
	return &Datasource{
		QueryDataHandler:    newQueryDataHandler(client),
		CallResourceHandler: newCallResourceHandler(client),
		CheckHealthHandler:  newCheckHealthHandler(client),
		InstanceDisposer:    disposerFunc(func() {}),
	}, nil
}

func newQueryDataHandler(client *pinotlib.PinotClient) backend.QueryDataHandler {
	return backend.QueryDataHandlerFunc(func(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
		var thisClient *pinotlib.PinotClient
		if token := req.GetHTTPHeader(backend.OAuthIdentityTokenHeaderName); token != "" {
			thisClient = client.WithAuthorization(token)
		} else {
			thisClient = client
		}

		response := backend.NewQueryDataResponse()
		for _, query := range req.Queries {
			log.FromContext(ctx).Debug("received query", "contents", string(query.JSON))
			response.Responses[query.RefID] = dataquery.ExecuteQuery(ctx, thisClient, query)
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

		var thisClient *pinotlib.PinotClient
		if token := req.GetHTTPHeader(backend.OAuthIdentityTokenHeaderName); token != "" {
			log.Info("!!! LOOK AT ME !!!", "token", token)
			thisClient = client.WithAuthorization(token)
		} else {
			thisClient = client
		}

		// Test connection to controller
		if tables, err := thisClient.ListTables(ctx); err != nil {
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
		if _, err := thisClient.ExecuteSqlQuery(ctx, pinotlib.NewSqlQuery("SELECT 1")); err != nil {
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

type disposerFunc func()

func (f disposerFunc) Dispose() { f() }
