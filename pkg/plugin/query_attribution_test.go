package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/dataquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type brokerRequest struct {
	Sql          string `json:"sql"`
	QueryOptions string `json:"queryOptions"`
}

// fakePinot serves just enough of the controller and broker API to run a query, and records each broker request.
type fakePinot struct {
	mu       sync.Mutex
	requests []brokerRequest
}

func newFakePinotClient(t *testing.T) (*pinot.Client, *fakePinot) {
	t.Helper()
	fake := &fakePinot{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/query/sql":
			var body brokerRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			fake.mu.Lock()
			fake.requests = append(fake.requests, body)
			fake.mu.Unlock()
			_, _ = w.Write([]byte(`{"exceptions":[],"resultTable":{"dataSchema":{"columnNames":["x"],"columnDataTypes":["STRING"]},"rows":[["a"]]}}`))
		case strings.HasSuffix(r.URL.Path, "/schema"):
			_, _ = w.Write([]byte(`{"schemaName":"t","dimensionFieldSpecs":[{"name":"x","dataType":"STRING"}]}`))
		case strings.HasPrefix(r.URL.Path, "/tables/"):
			_, _ = w.Write([]byte(`{"OFFLINE":{"tableName":"t_OFFLINE","tableType":"OFFLINE"}}`))
		default:
			_, _ = w.Write([]byte(`{"tables":[],"databases":[]}`))
		}
	}))
	t.Cleanup(server.Close)
	client := pinot.NewPinotClient(server.Client(), pinot.ClientProperties{BrokerUrl: server.URL, ControllerUrl: server.URL})
	return client, fake
}

func (fake *fakePinot) last(t *testing.T) brokerRequest {
	t.Helper()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.NotEmpty(t, fake.requests, "no broker request")
	return fake.requests[len(fake.requests)-1]
}

var qidField = regexp.MustCompile(`,"qid":"[0-9a-f]{16}"`)

// tagOf returns the clientQueryId a broker request carries with its qid removed, after checking that the SQL
// comment carries the same id. It returns "" for an untagged request.
func tagOf(t *testing.T, request brokerRequest) string {
	t.Helper()
	id, found := strings.CutPrefix(request.QueryOptions, "clientQueryId=")
	if !found {
		assert.False(t, strings.HasPrefix(request.Sql, "/*"), "untagged request has a comment: %q", request.Sql)
		return ""
	}
	assert.True(t, strings.HasPrefix(request.Sql, "/* clientQueryId: "+id+" */\n"), "sql %q", request.Sql)
	assert.Regexp(t, qidField, id)
	return qidField.ReplaceAllString(id, "")
}

func TestWithQueryAttribution(t *testing.T) {
	user := &backend.User{Login: "jdoe", Email: "jdoe@example.com", Name: "Jane Doe", Role: "Viewer"}

	tests := []struct {
		name   string
		mode   QueryAttributionMode
		user   *backend.User
		origin queryOrigin
		want   string // the clientQueryId sent without its qid, or "" for none
	}{
		{name: "unset", mode: "", user: user, want: ""},
		{name: "off", mode: QueryAttributionOff, user: user, want: ""},
		{name: "unknown value is off", mode: "login", user: user, want: ""},
		{name: "on", mode: QueryAttributionOn, user: user, want: `{"source":"grafana","login":"jdoe","email":"jdoe@example.com"}`},
		{name: "ON", mode: "ON", user: user, want: `{"source":"grafana","login":"jdoe","email":"jdoe@example.com"}`},
		{
			name: "on with dashboard and panel",
			mode: QueryAttributionOn, user: user, origin: queryOrigin{dashboardUid: "abc123", panelId: "4"},
			want: `{"source":"grafana","login":"jdoe","email":"jdoe@example.com","dashboardUid":"abc123","panelId":"4"}`,
		},
		{
			name: "alert rule without a user",
			mode: QueryAttributionOn, origin: queryOrigin{ruleUid: "rule1"},
			want: `{"source":"grafana","ruleUid":"rule1"}`,
		},
		{
			name: "user without email (service account)",
			mode: QueryAttributionOn, user: &backend.User{Login: "sa-1-svc"},
			want: `{"source":"grafana","login":"sa-1-svc"}`,
		},
		{name: "no user still tags source", mode: QueryAttributionOn, user: nil, want: `{"source":"grafana"}`},
		{
			name: "characters the broker can't take are escaped",
			mode: QueryAttributionOn, user: &backend.User{Login: "a;b=c*/d"},
			want: `{"source":"grafana","login":"a\u003bb\u003dc*\/d"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, fake := newFakePinotClient(t)
			ctx := withQueryAttribution(context.Background(), tt.mode, tt.user, tt.origin)
			_, err := client.ExecuteSqlQuery(ctx, pinot.NewSqlQuery("SELECT 1"))
			require.NoError(t, err)
			assert.Equal(t, tt.want, tagOf(t, fake.last(t)))
		})
	}

	t.Run("escaped value decodes to the original login", func(t *testing.T) {
		client, fake := newFakePinotClient(t)
		ctx := withQueryAttribution(context.Background(), QueryAttributionOn, &backend.User{Login: "a;b=c*/d"}, queryOrigin{})
		_, err := client.ExecuteSqlQuery(ctx, pinot.NewSqlQuery("SELECT 1"))
		require.NoError(t, err)
		var got queryAttribution
		require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(fake.last(t).QueryOptions, "clientQueryId=")), &got))
		assert.Equal(t, "a;b=c*/d", got.Login)
	})

	t.Run("each query gets its own id", func(t *testing.T) {
		client, fake := newFakePinotClient(t)
		ctx := withQueryAttribution(context.Background(), QueryAttributionOn, user, queryOrigin{})
		for range 2 {
			_, err := client.ExecuteSqlQuery(ctx, pinot.NewSqlQuery("SELECT 1"))
			require.NoError(t, err)
		}
		require.Len(t, fake.requests, 2)
		assert.NotEqual(t, fake.requests[0].QueryOptions, fake.requests[1].QueryOptions)
		assert.Equal(t, tagOf(t, fake.requests[0]), tagOf(t, fake.requests[1]))
	})
}

func TestQueryAttributionHandlers(t *testing.T) {
	user := &backend.User{Login: "jdoe", Email: "jdoe@example.com"}
	pluginContext := backend.PluginContext{User: user}

	t.Run("data query from a dashboard panel", func(t *testing.T) {
		client, fake := newFakePinotClient(t)
		handler := newQueryDataHandler(client, dataquery.DataSourceMeta{}, QueryAttributionOn)
		resp, err := handler.QueryData(context.Background(), &backend.QueryDataRequest{
			PluginContext: pluginContext,
			Headers:       map[string]string{"http_X-Dashboard-Uid": "dash1", "http_X-Panel-Id": "7"},
			Queries: []backend.DataQuery{{RefID: "A", Interval: time.Minute, TimeRange: lastHour(), JSON: json.RawMessage(
				`{"queryType":"PinotQL","editorMode":"Code","displayType":"TABLE","tableName":"t","pinotQlCode":"SELECT x FROM t"}`)}},
		})
		require.NoError(t, err)
		require.NoError(t, resp.Responses["A"].Error)
		assert.Equal(t, `{"source":"grafana","login":"jdoe","email":"jdoe@example.com","dashboardUid":"dash1","panelId":"7"}`,
			tagOf(t, fake.last(t)))
	})

	t.Run("data query from an alert rule", func(t *testing.T) {
		client, fake := newFakePinotClient(t)
		handler := newQueryDataHandler(client, dataquery.DataSourceMeta{}, QueryAttributionOn)
		resp, err := handler.QueryData(context.Background(), &backend.QueryDataRequest{
			Headers: map[string]string{"http_X-Rule-Uid": "rule1"},
			Queries: []backend.DataQuery{{RefID: "A", Interval: time.Minute, TimeRange: lastHour(), JSON: json.RawMessage(
				`{"queryType":"PinotQL","editorMode":"Code","displayType":"TABLE","tableName":"t","pinotQlCode":"SELECT x FROM t"}`)}},
		})
		require.NoError(t, err)
		require.NoError(t, resp.Responses["A"].Error)
		assert.Equal(t, `{"source":"grafana","ruleUid":"rule1"}`, tagOf(t, fake.last(t)))
	})

	t.Run("resource query", func(t *testing.T) {
		client, fake := newFakePinotClient(t)
		handler := newCallResourceHandler(client, QueryAttributionOn)
		var status int
		err := handler.CallResource(context.Background(), &backend.CallResourceRequest{
			PluginContext: pluginContext,
			Method:        http.MethodPost,
			Path:          "query/distinctValues",
			URL:           "query/distinctValues",
			Body:          bytes.NewBufferString(`{"tableName":"t","columnName":"x"}`).Bytes(),
		}, backend.CallResourceResponseSenderFunc(func(resp *backend.CallResourceResponse) error {
			status = resp.Status
			return nil
		}))
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, `{"source":"grafana","login":"jdoe","email":"jdoe@example.com"}`, tagOf(t, fake.last(t)))
	})

	t.Run("off", func(t *testing.T) {
		client, fake := newFakePinotClient(t)
		handler := newQueryDataHandler(client, dataquery.DataSourceMeta{}, QueryAttributionOff)
		_, err := handler.QueryData(context.Background(), &backend.QueryDataRequest{
			PluginContext: pluginContext,
			Queries: []backend.DataQuery{{RefID: "A", Interval: time.Minute, TimeRange: lastHour(), JSON: json.RawMessage(
				`{"queryType":"PinotQL","editorMode":"Code","displayType":"TABLE","tableName":"t","pinotQlCode":"SELECT x FROM t"}`)}},
		})
		require.NoError(t, err)
		assert.Equal(t, "", tagOf(t, fake.last(t)))
	})
}

func TestConfig_ReadFrom_QueryAttribution(t *testing.T) {
	tests := []struct {
		jsonValue   string
		wantEnabled bool
		wantKnown   bool
	}{
		{jsonValue: `null`, wantEnabled: false, wantKnown: true},
		{jsonValue: `""`, wantEnabled: false, wantKnown: true},
		{jsonValue: `"off"`, wantEnabled: false, wantKnown: true},
		{jsonValue: `"on"`, wantEnabled: true, wantKnown: true},
		{jsonValue: `"ON"`, wantEnabled: true, wantKnown: true},
		// Grafana 9 reads an unquoted on/off in a provisioning file as a YAML boolean.
		{jsonValue: `true`, wantEnabled: true, wantKnown: true},
		{jsonValue: `false`, wantEnabled: false, wantKnown: true},
		{jsonValue: `"true"`, wantEnabled: true, wantKnown: true},
		// Unknown values don't stop the datasource loading; they mean off.
		{jsonValue: `"name"`, wantEnabled: false, wantKnown: false},
		{jsonValue: `1`, wantEnabled: false, wantKnown: false},
	}
	for _, tt := range tests {
		t.Run(tt.jsonValue, func(t *testing.T) {
			var config Config
			require.NoError(t, config.ReadFrom(attributionSettings(tt.jsonValue)))
			assert.Equal(t, tt.wantEnabled, config.QueryAttribution.Enabled())
			assert.Equal(t, tt.wantKnown, config.QueryAttribution.Known())
		})
	}

	t.Run("missing", func(t *testing.T) {
		var config Config
		require.NoError(t, config.ReadFrom(backend.DataSourceInstanceSettings{
			JSONData: json.RawMessage(`{"brokerUrl":"http://localhost:8000","controllerUrl":"http://localhost:9000"}`),
		}))
		assert.False(t, config.QueryAttribution.Enabled())
	})
}

func attributionSettings(jsonValue string) backend.DataSourceInstanceSettings {
	return backend.DataSourceInstanceSettings{
		JSONData: json.RawMessage(
			`{"brokerUrl":"http://localhost:8000","controllerUrl":"http://localhost:9000","queryAttribution":` + jsonValue + `}`),
		DecryptedSecureJSONData: map[string]string{},
	}
}

func lastHour() backend.TimeRange {
	now := time.Now()
	return backend.TimeRange{From: now.Add(-time.Hour), To: now}
}
