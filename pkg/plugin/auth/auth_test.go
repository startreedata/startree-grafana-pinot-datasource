package auth

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestPassThroughOAuth(t *testing.T) {
	client := pinotlib.NewPinotClient(http.DefaultClient, pinotlib.PinotClientProperties{
		Authorization: "Bearer base_token",
	})

	for _, tt := range []struct {
		name          string
		authorization string
		want          string
	}{
		{name: "empty", authorization: "", want: "Bearer base_token"},
		{name: "present", authorization: "Bearer token", want: "Bearer token"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("*http.Request", func(t *testing.T) {
				req, err := http.NewRequest("GET", "http://example.com", nil)
				require.NoError(t, err)
				req.Header.Set("Authorization", tt.authorization)
				assert.Equal(t, tt.want, PassThroughOAuth(client, req).Properties().Authorization)
			})
			t.Run("*backend.QueryDataRequest", func(t *testing.T) {
				req := new(backend.QueryDataRequest)
				req.SetHTTPHeader(backend.OAuthIdentityTokenHeaderName, tt.authorization)
				assert.Equal(t, tt.want, PassThroughOAuth(client, req).Properties().Authorization)
			})
			t.Run("*backend.CheckHealthRequest", func(t *testing.T) {
				req := new(backend.CheckHealthRequest)
				req.SetHTTPHeader(backend.OAuthIdentityTokenHeaderName, tt.authorization)
				assert.Equal(t, tt.want, PassThroughOAuth(client, req).Properties().Authorization)
			})
		})
	}
}
