package plugin

import (
	"context"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/dataquery"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
)

func NewQueryDataHandler(client *pinotlib.PinotClient) backend.QueryDataHandler {
	return backend.QueryDataHandlerFunc(func(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
		response := backend.NewQueryDataResponse()
		for _, query := range req.Queries {
			backend.Logger.Debug(fmt.Sprintf("received query: %s", string(query.JSON)))
			response.Responses[query.RefID] = fetchData(ctx, client, query)
		}
		return response, nil
	})
}

func fetchData(ctx context.Context, client *pinotlib.PinotClient, query backend.DataQuery) backend.DataResponse {
	pinotDataQuery, err := dataquery.PinotDataQueryFrom(query)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, err.Error())
	}
	return dataquery.ExecutableQueryFrom(pinotDataQuery).Execute(ctx, client)
}
