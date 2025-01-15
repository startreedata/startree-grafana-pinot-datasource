package plugin

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/dataquery"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
)

func NewQueryDataHandler(client *pinotlib.PinotClient) backend.QueryDataHandler {
	return backend.QueryDataHandlerFunc(func(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
		response := backend.NewQueryDataResponse()
		for _, query := range req.Queries {
			log.Debug("received query", "contents", string(query.JSON))
			response.Responses[query.RefID] = dataquery.ExecuteQuery(ctx, client, query)
		}
		return response, nil
	})
}
