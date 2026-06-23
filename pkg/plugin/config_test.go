package plugin

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

func TestConfig_ReadFrom_TLS(t *testing.T) {
	settings := backend.DataSourceInstanceSettings{
		JSONData: json.RawMessage(`{
			"brokerUrl":"https://broker:8000",
			"controllerUrl":"https://controller:9000",
			"tlsSkipVerify":true,
			"tlsAuth":true,
			"tlsAuthWithCACert":true,
			"serverName":"pinot.example.com"
		}`),
		DecryptedSecureJSONData: map[string]string{},
	}

	var got Config
	assert.NoError(t, got.ReadFrom(settings))
	assert.Equal(t, Config{
		ControllerUrl:     "https://controller:9000",
		BrokerUrl:         "https://broker:8000",
		TLSSkipVerify:     true,
		TLSAuth:           true,
		TLSAuthWithCACert: true,
		TLSServerName:     "pinot.example.com",
	}, got)
}
