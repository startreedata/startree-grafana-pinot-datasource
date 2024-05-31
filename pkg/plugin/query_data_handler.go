package plugin

import (
	"context"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

func NewQueryDataHandler(client *PinotClient) backend.QueryDataHandler {
	return backend.QueryDataHandlerFunc(func(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
		response := backend.NewQueryDataResponse()
		for _, query := range req.Queries {
			backend.Logger.Info(fmt.Sprintf("received query: %s", string(query.JSON)))
			response.Responses[query.RefID] = fetchData(client, ctx, query)
		}
		return response, nil
	})
}

func fetchData(client *PinotClient, ctx context.Context, query backend.DataQuery) backend.DataResponse {
	pinotDataQuery, err := PinotDataQueryFrom(query)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, err.Error())
	}

	var tableSchema TableSchema
	if pinotDataQuery.TableName != "" {
		tableSchema, err = client.GetTableSchema(ctx, pinotDataQuery.DatabaseName, pinotDataQuery.TableName)
		if err != nil {
			return backend.ErrDataResponse(backend.StatusInternal, err.Error())
		}
	}

	errorMessageFor := func(err error) string {
		return fmt.Sprintf("Error: %s.", err.Error())
	}

	driver, err := NewDriver(pinotDataQuery, tableSchema, query.TimeRange)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, errorMessageFor(err))
	}
	sql, err := driver.RenderPinotSql()
	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, errorMessageFor(err))
	}

	resp, err := client.ExecuteSQL(ctx, pinotDataQuery.TableName, sql)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, errorMessageFor(err))
	}

	results, err := driver.ExtractResults(resp.ResultTable)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, errorMessageFor(err))
	}

	return backend.DataResponse{Frames: data.Frames{results}}
}
