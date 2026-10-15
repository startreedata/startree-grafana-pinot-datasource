package plugin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/dataquery"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatasource(t *testing.T) {
	instance, err := NewInstance(context.Background(), backend.DataSourceInstanceSettings{
		JSONData: json.RawMessage(
			`{"brokerUrl":"http://localhost:8000","controllerUrl":"http://localhost:9000","tokenType":"bearer"}`),
		DecryptedSecureJSONData: map[string]string{},
	})
	require.NoError(t, err)

	require.NotNil(t, instance)
	pinotDatasource := instance.(*Datasource)
	assert.NotNil(t, pinotDatasource.CallResourceHandler)
	assert.NotNil(t, pinotDatasource.CheckHealthHandler)
	assert.NotNil(t, pinotDatasource.QueryDataHandler)
	assert.NotNil(t, pinotDatasource.InstanceDisposer)
}

func TestCheckHealthBrokerExceptions(t *testing.T) {
	for _, tc := range []struct {
		name        string
		response    string
		wantStatus  backend.HealthStatus
		wantMessage string
	}{
		{
			name:        "successful query",
			response:    `{"exceptions":[],"resultTable":{"rows":[[1]]}}`,
			wantStatus:  backend.HealthStatusOk,
			wantMessage: "Pinot data source is working",
		},
		{
			name:        "HTTP 200 with broker exceptions",
			response:    `{"exceptions":[{"errorCode":150,"message":"Access denied"}]}`,
			wantStatus:  backend.HealthStatusError,
			wantMessage: "Broker request completed with exceptions:\nCode 150: Access denied",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/mytables":
					_, _ = w.Write([]byte(`{"tables":["test"]}`))
				case "/query/sql":
					_, _ = w.Write([]byte(tc.response))
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := pinot.NewPinotClient(server.Client(), pinot.ClientProperties{
				ControllerUrl: server.URL,
				BrokerUrl:     server.URL,
			})

			result, err := newCheckHealthHandler(client).CheckHealth(context.Background(), &backend.CheckHealthRequest{})
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, result.Status)
			assert.Equal(t, tc.wantMessage, result.Message)
		})
	}
}

func TestQueryData(t *testing.T) {
	client := test_helpers.SetupPinotAndCreateClient(t)

	handler := newQueryDataHandler(client, dataquery.DataSourceMeta{})
	resp, err := handler.QueryData(
		context.Background(),
		&backend.QueryDataRequest{
			Queries: []backend.DataQuery{
				{RefID: "A"},
			},
		},
	)
	if err != nil {
		t.Error(err)
	}

	if len(resp.Responses) != 1 {
		t.Fatal("QueryData must return a response")
	}
}
