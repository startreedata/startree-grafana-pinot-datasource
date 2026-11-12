package plugin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// brokerRequestFor runs a query on ctx against a fake broker and returns the sql and query options it received.
func brokerRequestFor(t *testing.T, ctx context.Context) (sql string, queryOptions string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Sql          string `json:"sql"`
			QueryOptions string `json:"queryOptions"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		sql, queryOptions = body.Sql, body.QueryOptions
		_, _ = w.Write([]byte(`{"exceptions":[]}`))
	}))
	defer server.Close()

	client := pinot.NewPinotClient(server.Client(), pinot.ClientProperties{BrokerUrl: server.URL})
	_, err := client.ExecuteSqlQuery(ctx, pinot.NewSqlQuery("SELECT 1"))
	require.NoError(t, err)
	return sql, queryOptions
}

func TestWithQueryAttribution(t *testing.T) {
	user := &backend.User{Login: "jdoe", Email: "jdoe@example.com", Name: "Jane Doe", Role: "Viewer"}

	tests := []struct {
		name      string
		mode      string
		user      *backend.User
		dashboard string
		panel     string
		want      string // the clientQueryId sent, or "" for none
	}{
		{name: "unset", mode: "", user: user, want: ""},
		{name: "off", mode: QueryAttributionOff, user: user, want: ""},
		{name: "on", mode: QueryAttributionOn, user: user, want: `{"source":"grafana","login":"jdoe","email":"jdoe@example.com"}`},
		{
			name: "on with dashboard and panel",
			mode: QueryAttributionOn, user: user, dashboard: "abc123", panel: "4",
			want: `{"source":"grafana","login":"jdoe","email":"jdoe@example.com","dashboardUid":"abc123","panelId":"4"}`,
		},
		{
			name: "user without email (service account)",
			mode: QueryAttributionOn, user: &backend.User{Login: "sa-1-svc"},
			want: `{"source":"grafana","login":"sa-1-svc"}`,
		},
		{name: "legacy login value means on", mode: "login", user: user, want: `{"source":"grafana","login":"jdoe","email":"jdoe@example.com"}`},
		{name: "legacy email value means on", mode: "email", user: user, want: `{"source":"grafana","login":"jdoe","email":"jdoe@example.com"}`},
		{name: "no user still tags source", mode: QueryAttributionOn, user: nil, want: `{"source":"grafana"}`},
		{
			name: "characters the broker can't take are escaped",
			mode: QueryAttributionOn, user: &backend.User{Login: "a;b=c*/d"},
			want: `{"source":"grafana","login":"a\u003bb\u003dc*\/d"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := withQueryAttribution(context.Background(), tt.mode, tt.user, tt.dashboard, tt.panel)
			sql, queryOptions := brokerRequestFor(t, ctx)
			if tt.want == "" {
				assert.Equal(t, "SELECT 1", sql)
				assert.Empty(t, queryOptions)
				return
			}
			assert.Equal(t, "/* clientQueryId: "+tt.want+" */\nSELECT 1", sql)
			assert.Equal(t, "clientQueryId="+tt.want, queryOptions)
		})
	}

	t.Run("escaped value decodes to the original login", func(t *testing.T) {
		ctx := withQueryAttribution(context.Background(), QueryAttributionOn, &backend.User{Login: "a;b=c*/d"}, "", "")
		_, queryOptions := brokerRequestFor(t, ctx)
		var got queryAttribution
		require.NoError(t, json.Unmarshal([]byte(queryOptions[len("clientQueryId="):]), &got))
		assert.Equal(t, "a;b=c*/d", got.Login)
	})
}

func TestConfig_ReadFrom_QueryAttribution(t *testing.T) {
	for _, mode := range []string{"", QueryAttributionOff, QueryAttributionOn, "login", "email"} {
		t.Run("valid "+mode, func(t *testing.T) {
			var config Config
			require.NoError(t, config.ReadFrom(attributionSettings(mode)))
			assert.Equal(t, mode, config.QueryAttribution)
		})
	}

	t.Run("invalid", func(t *testing.T) {
		var config Config
		assert.ErrorContains(t, config.ReadFrom(attributionSettings("name")), `invalid queryAttribution "name"`)
	})
}

func attributionSettings(mode string) backend.DataSourceInstanceSettings {
	jsonData, _ := json.Marshal(map[string]string{
		"brokerUrl":        "http://localhost:8000",
		"controllerUrl":    "http://localhost:9000",
		"queryAttribution": mode,
	})
	return backend.DataSourceInstanceSettings{JSONData: jsonData, DecryptedSecureJSONData: map[string]string{}}
}
