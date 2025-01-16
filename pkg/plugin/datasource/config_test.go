package datasource

import (
	"encoding/json"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConfig_ReadFrom(t *testing.T) {
	settings := backend.DataSourceInstanceSettings{
		JSONData: json.RawMessage(
			`{"brokerUrl":"http://localhost:8000","controllerUrl":"http://localhost:9000","tokenType":"Bearer"}`),
		DecryptedSecureJSONData: map[string]string{},
	}

	var got Config
	assert.NoError(t, got.ReadFrom(settings))
	assert.Equal(t, Config{
		ControllerUrl: "http://localhost:9000",
		BrokerUrl:     "http://localhost:8000",
		TokenType:     "Bearer",
	}, got)
}
