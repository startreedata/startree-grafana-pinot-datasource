package auth

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"net/http"
)

func PinotClientFor[T *http.Request | *backend.QueryDataRequest | *backend.CheckHealthRequest](client *pinot.Client, ctx context.Context, req T) *pinot.Client {
	var auth string
	switch r := any(req).(type) {
	case *http.Request:
		auth = r.Header.Get("Authorization")
	case *backend.QueryDataRequest:
		auth = r.GetHTTPHeader(backend.OAuthIdentityTokenHeaderName)
	case *backend.CheckHealthRequest:
		auth = r.GetHTTPHeader(backend.OAuthIdentityTokenHeaderName)
	}

	logger := log.FromContext(ctx)
	if auth != "" {
		return client.WithAuthorization(auth).WithLogger(logger.With("oauthPassThru", true))
	} else {
		return client.WithLogger(logger)
	}
}
