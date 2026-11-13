package plugin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
)

// Query attribution tags each broker query with the Grafana user's login and email (and the dashboard/panel or
// alert rule when known) via Pinot's clientQueryId option and a SQL comment, so shared-token deployments can
// tell who ran a query from the system_query_log clientRequestId column or the broker logs.
const (
	QueryAttributionOff QueryAttributionMode = "off"
	QueryAttributionOn  QueryAttributionMode = "on"
)

// QueryAttributionMode is the datasource's queryAttribution setting.
type QueryAttributionMode string

// UnmarshalJSON accepts booleans as well as strings. Grafana 9 reads an unquoted on/off in a provisioning file as
// a YAML boolean, and a UI switch suggests true/false to anyone writing the config by hand or with Terraform.
func (mode *QueryAttributionMode) UnmarshalJSON(data []byte) error {
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	switch value := value.(type) {
	case bool:
		*mode = QueryAttributionOff
		if value {
			*mode = QueryAttributionOn
		}
	case string:
		*mode = QueryAttributionMode(value)
	case nil:
		*mode = ""
	default:
		*mode = QueryAttributionMode(data)
	}
	return nil
}

// Enabled reports whether queries are tagged. Values other than on/off (any case, or true/false) are off, so a
// typo or a value from a newer plugin version can't stop the datasource loading; see Known.
func (mode QueryAttributionMode) Enabled() bool {
	switch strings.ToLower(string(mode)) {
	case "on", "true":
		return true
	}
	return false
}

// Known reports whether mode is a value this version understands.
func (mode QueryAttributionMode) Known() bool {
	switch strings.ToLower(string(mode)) {
	case "", "on", "off", "true", "false":
		return true
	}
	return false
}

// Grafana forwards these on data queries issued from a dashboard panel, and from an alert rule evaluation.
const (
	dashboardUidHeader = "X-Dashboard-Uid"
	panelIdHeader      = "X-Panel-Id"
	ruleUidHeader      = "X-Rule-Uid"
)

type queryAttribution struct {
	Source       string `json:"source"`
	Login        string `json:"login,omitempty"`
	Email        string `json:"email,omitempty"`
	DashboardUid string `json:"dashboardUid,omitempty"`
	PanelId      string `json:"panelId,omitempty"`
	RuleUid      string `json:"ruleUid,omitempty"`
	// Qid makes each query's clientQueryId unique: the broker looks running queries up by it, e.g. to cancel one.
	Qid string `json:"qid"`
}

// queryOrigin is where a query came from, as far as Grafana tells the plugin.
type queryOrigin struct {
	dashboardUid, panelId, ruleUid string
}

// withQueryAttribution returns ctx tagged with the attribution clientQueryId when attribution is on.
// Login and email are both sent: the login is always set and unique, while the email may be empty (service
// accounts) or change. Requests without a user identity (e.g. alerting) are still tagged as coming from Grafana.
func withQueryAttribution(ctx context.Context, mode QueryAttributionMode, user *backend.User, origin queryOrigin) context.Context {
	if !mode.Enabled() {
		return ctx
	}
	attribution := queryAttribution{
		Source:       "grafana",
		DashboardUid: origin.dashboardUid,
		PanelId:      origin.panelId,
		RuleUid:      origin.ruleUid,
	}
	if user != nil {
		attribution.Login, attribution.Email = user.Login, user.Email
	}
	return pinot.WithClientQueryId(ctx, func() string {
		tag := attribution // A copy: queries on one context can run concurrently.
		tag.Qid = newQid()
		id, err := json.Marshal(tag)
		if err != nil {
			// Unreachable for a struct of strings; never block a query on attribution.
			return ""
		}
		return unsplittableJson.Replace(string(id))
	})
}

func newQid() string {
	var b [8]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error.
	return hex.EncodeToString(b[:])
}

// unsplittableJson rewrites the characters pinot.ClientQueryIdSafe rejects as equivalent JSON escapes.
// They can only occur inside JSON strings here, so the value still decodes to the same object.
var unsplittableJson = strings.NewReplacer(";", "\\u003b", "=", "\\u003d", "*/", "*\\/")
