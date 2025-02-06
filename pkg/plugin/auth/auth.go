package auth

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"net/http"
)

func PassThroughOAuth[T *http.Request | *backend.QueryDataRequest | *backend.CheckHealthRequest](client *pinotlib.PinotClient, req T) *pinotlib.PinotClient {
	var auth string
	switch r := any(req).(type) {
	case *http.Request:
		auth = r.Header.Get("Authorization")
	case *backend.QueryDataRequest:
		auth = r.GetHTTPHeader(backend.OAuthIdentityTokenHeaderName)
	case *backend.CheckHealthRequest:
		auth = r.GetHTTPHeader(backend.OAuthIdentityTokenHeaderName)
	}

	if auth != "" {
		return client.WithAuthorization(auth)
	} else {
		return client
	}
}
