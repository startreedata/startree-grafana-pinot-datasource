package plugin

import (
	"context"
	"encoding/json"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
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

func TestQueryData(t *testing.T) {
	client := test_helpers.SetupPinotAndCreateClient(t)

	handler := newQueryDataHandler(client)
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
