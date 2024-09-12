package plugin

import (
	"encoding/json"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDatasource(t *testing.T) {
	datasource, err := NewDatasource(backend.DataSourceInstanceSettings{
		JSONData: json.RawMessage(
			`{"brokerUrl":"http://localhost:8000","controllerUrl":"http://localhost:9000","tokenType":"bearer"}`),
		DecryptedSecureJSONData: map[string]string{},
	})
	require.NoError(t, err)

	require.NotNil(t, datasource)
	pinotDatasource := datasource.(*Datasource)
	assert.NotNil(t, pinotDatasource.CallResourceHandler)
	assert.NotNil(t, pinotDatasource.CheckHealthHandler)
	assert.NotNil(t, pinotDatasource.QueryDataHandler)
}
