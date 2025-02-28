package pinot

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot/pinottest"
	"github.com/stretchr/testify/assert"
	"log/slog"
	"net/http"
	"os"
	"testing"
)

func TestNewPinotClient(t *testing.T) {
	t.Run("with default values", func(t *testing.T) {
		got := NewPinotClient(http.DefaultClient, ClientProperties{})
		assert.Equal(t, ClientProperties{}, got.properties)
		assert.Equal(t, http.DefaultClient, got.httpClient)
		assert.Equal(t, map[string]string{}, got.headers)
		assert.Equal(t, slog.Default(), got.logger)
	})

	t.Run("with custom values", func(t *testing.T) {
		got := NewPinotClient(http.DefaultClient, ClientProperties{
			ControllerUrl: "http://localhost:9000",
			BrokerUrl:     "http://localhost:8000",
			DatabaseName:  "test_db",
			Authorization: "Bearer test_token",
		})
		assert.Equal(t, ClientProperties{
			ControllerUrl: "http://localhost:9000",
			BrokerUrl:     "http://localhost:8000",
			DatabaseName:  "test_db",
			Authorization: "Bearer test_token",
		}, got.properties)
		assert.Equal(t, http.DefaultClient, got.httpClient)
		assert.Equal(t, map[string]string{"Database": "test_db", "Authorization": "Bearer test_token"}, got.headers)
	})
}

func TestPinotClient_WithAuthorization(t *testing.T) {
	got := NewPinotClient(http.DefaultClient, ClientProperties{
		ControllerUrl: "http://localhost:9000",
		BrokerUrl:     "http://localhost:8000",
	}).WithAuthorization("Bearer test_token")

	assert.Equal(t, ClientProperties{
		ControllerUrl: "http://localhost:9000",
		BrokerUrl:     "http://localhost:8000",
		Authorization: "Bearer test_token",
	}, got.properties)
	assert.Equal(t, map[string]string{"Authorization": "Bearer test_token"}, got.headers)
}

func TestPinotClient_WithLogger(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	got := NewPinotClient(http.DefaultClient, ClientProperties{
		ControllerUrl: pinottest.ControllerUrl,
		BrokerUrl:     pinottest.BrokerUrl,
	}).WithLogger(logger)
	assert.Equal(t, logger, got.logger)
}

func setupPinotAndCreateClient(t *testing.T) *Client {
	pinottest.CreateTestTables()
	pinotClient := NewPinotClient(http.DefaultClient, ClientProperties{
		ControllerUrl: pinottest.ControllerUrl,
		BrokerUrl:     pinottest.BrokerUrl,
	})
	return pinotClient
}
