package plugin

import (
	"context"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/resource/httpadapter"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/dataquery"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/resources"
	"net/http"
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
	httpClient  *http.Client
	pinotClient *pinot.Client
}

func NewInstance(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	var config Config
	if err := config.ReadFrom(settings); err != nil {
		return nil, err
	}

	// Create HTTP client options from datasource settings
	opts, err := settings.HTTPClientOptions(ctx)
	if err != nil {
		return nil, fmt.Errorf("http client options: %w", err)
	}

	// Create HTTP client WITHOUT header forwarding for internal plugin resources
	opts.ForwardHTTPHeaders = false
	httpClient, err := httpclient.New(opts)
	if err != nil {
		return nil, fmt.Errorf("httpclient new: %w", err)
	}

	// Create HTTP client WITH header forwarding for external Pinot API calls
	opts.ForwardHTTPHeaders = true
	pinotHttpClient, err := httpclient.New(opts)
	if err != nil {
		return nil, fmt.Errorf("pinot httpclient new: %w", err)
	}

	// Create Pinot client with the OAuth-enabled HTTP client for external API calls
	pinotClient := PinotClientOf(pinotHttpClient, config)
	
	// Create Pinot client with internal HTTP client for resource operations (SQL preview, etc.)
	internalPinotClient := PinotClientOf(httpClient, config)

	return &Datasource{
		QueryDataHandler:    newQueryDataHandler(pinotClient),
		CallResourceHandler: newCallResourceHandler(internalPinotClient),
		CheckHealthHandler:  newCheckHealthHandler(pinotClient),
		InstanceDisposer:    disposerFunc(func() {}),
		httpClient:          httpClient,
		pinotClient:         pinotClient,
	}, nil
}

func newQueryDataHandler(client *pinot.Client) backend.QueryDataHandler {
	return backend.QueryDataHandlerFunc(func(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// OAuth pass-through is now handled automatically by the SDK HTTP client
		resp := backend.NewQueryDataResponse()
		for _, query := range req.Queries {
			log.FromContext(ctx).Debug("received Pinot data query", "contents", string(query.JSON))
			resp.Responses[query.RefID] = dataquery.ExecuteQuery(client, ctx, query)
		}
		return resp, nil
	})
}

func newCallResourceHandler(client *pinot.Client) backend.CallResourceHandler {
	return httpadapter.New(resources.NewResourceHandler(client))
}

func newCheckHealthHandler(client *pinot.Client) backend.CheckHealthHandler {
	return backend.CheckHealthHandlerFunc(func(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// OAuth pass-through is now handled automatically by the SDK HTTP client

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
		if _, err := client.ExecuteSqlQuery(ctx, pinot.NewSqlQuery("SELECT 1")); err != nil {
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
