package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
)

// Query attribution tags each broker query with the Grafana user's login and email (and dashboard/panel when
// known) via Pinot's clientQueryId option and a SQL comment, so shared-token deployments can tell who ran a
// query from the system_query_log clientRequestId column or the broker logs.
const (
	QueryAttributionOff = "off"
	QueryAttributionOn  = "on"
	// Values written by earlier builds of this feature, which picked one identifier. Both now mean "on".
	queryAttributionLegacyLogin = "login"
	queryAttributionLegacyEmail = "email"
)

func validQueryAttribution(mode string) bool {
	switch mode {
	case "", QueryAttributionOff, QueryAttributionOn, queryAttributionLegacyLogin, queryAttributionLegacyEmail:
		return true
	}
	return false
}

func queryAttributionEnabled(mode string) bool {
	return mode == QueryAttributionOn || mode == queryAttributionLegacyLogin || mode == queryAttributionLegacyEmail
}

// Grafana forwards these on data queries issued from a dashboard panel.
const (
	dashboardUidHeader = "X-Dashboard-Uid"
	panelIdHeader      = "X-Panel-Id"
)

type queryAttribution struct {
	Source       string `json:"source"`
	Login        string `json:"login,omitempty"`
	Email        string `json:"email,omitempty"`
	DashboardUid string `json:"dashboardUid,omitempty"`
	PanelId      string `json:"panelId,omitempty"`
}

// withQueryAttribution returns ctx tagged with the attribution clientQueryId when attribution is on.
// Login and email are both sent: the login is always set and unique, while the email may be empty (service
// accounts) or change. Requests without a user identity (e.g. alerting) are still tagged as coming from Grafana.
func withQueryAttribution(ctx context.Context, mode string, user *backend.User, dashboardUid, panelId string) context.Context {
	if !queryAttributionEnabled(mode) {
		return ctx
	}
	attribution := queryAttribution{Source: "grafana", DashboardUid: dashboardUid, PanelId: panelId}
	if user != nil {
		attribution.Login, attribution.Email = user.Login, user.Email
	}
	id, err := json.Marshal(attribution)
	if err != nil {
		// Unreachable for a struct of strings; never block a query on attribution.
		return ctx
	}
	return pinot.WithClientQueryId(ctx, unsplittableJson.Replace(string(id)))
}

// unsplittableJson rewrites the characters pinot.WithClientQueryId can't send as equivalent JSON escapes.
// They can only occur inside JSON strings here, so the value still decodes to the same object.
var unsplittableJson = strings.NewReplacer(";", "\\u003b", "=", "\\u003d", "*/", "*\\/")

func errInvalidQueryAttribution(mode string) error {
	return fmt.Errorf("invalid queryAttribution %q: must be %q or %q", mode, QueryAttributionOff, QueryAttributionOn)
}
