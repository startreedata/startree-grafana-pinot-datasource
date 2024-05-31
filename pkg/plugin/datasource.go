package plugin

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
)

// Logger with context for this plugin.
// TODO: Add additional logging context as needed.
var Logger = backend.Logger

var (
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ backend.CallResourceHandler   = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

type Datasource struct {
	backend.CallResourceHandler
	backend.CheckHealthHandler
	backend.QueryDataHandler
	instancemgmt.InstanceDisposer
}

func NewDatasource(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	pinotProperties, err := PinotPropertiesFrom(settings)
	if err != nil {
		return nil, err
	}

	client, err := NewPinotClient(pinotProperties)
	if err != nil {
		return nil, err
	}

	return &Datasource{
		CallResourceHandler: NewCallResourceHandler(client),
		CheckHealthHandler:  NewCheckHealthHandler(client),
		QueryDataHandler:    NewQueryDataHandler(client),
		InstanceDisposer:    NewInstanceDisposer(),
	}, nil
}
