package plugin

import (
	"encoding/json"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestConfig_CheckUrlCredentials(t *testing.T) {
	for _, tc := range []struct {
		name    string
		json    string
		wantErr string
	}{
		{
			name:    "broker user and password",
			json:    `{"brokerUrl":"http://admin:s3cret@localhost:8000","controllerUrl":"http://localhost:9000"}`,
			wantErr: "Broker URL must not contain a username or password",
		},
		{
			name:    "controller user only",
			json:    `{"brokerUrl":"http://localhost:8000","controllerUrl":"https://admin@localhost:9000"}`,
			wantErr: "Controller URL must not contain a username or password",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got Config
			require.NoError(t, got.ReadFrom(backend.DataSourceInstanceSettings{JSONData: json.RawMessage(tc.json)}))
			err := got.CheckUrlCredentials()
			assert.ErrorContains(t, err, tc.wantErr)
			assert.ErrorContains(t, err, "Token type to Basic")
			assert.NotContains(t, err.Error(), "s3cret")
		})
	}
}

func TestConfig_ReadFrom_NullPayload(t *testing.T) {
	var got Config
	assert.NotPanics(t, func() {
		assert.ErrorContains(t, got.ReadFrom(backend.DataSourceInstanceSettings{JSONData: json.RawMessage(`null`)}), "broker url cannot be empty")
	})
}
