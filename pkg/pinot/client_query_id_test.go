package pinot

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

// captureBrokerClient returns a client whose broker records the last request it receives.
func captureBrokerClient(t *testing.T, props ClientProperties) (*Client, *sqlRequest) {
	t.Helper()
	var got sqlRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		_, _ = w.Write([]byte(`{"exceptions":[]}`))
	}))
	t.Cleanup(server.Close)
	props.BrokerUrl = server.URL
	return NewPinotClient(server.Client(), props), &got
}

func TestPinotClient_ExecuteSqlQuery_ClientQueryId(t *testing.T) {
	tests := []struct {
		name       string
		id         string
		clientOpts []QueryOption
		queryOpts  []QueryOption
		want       sqlRequest
	}{
		{
			name: "untagged",
			want: sqlRequest{Sql: "SELECT 1"},
		},
		{
			name: "tagged",
			id:   `{"user":"jdoe"}`,
			want: sqlRequest{
				Sql:          "/* clientQueryId: {\"user\":\"jdoe\"} */\nSELECT 1",
				QueryOptions: `clientQueryId={"user":"jdoe"}`,
			},
		},
		{
			name:       "tagged with other options",
			id:         "jdoe",
			clientOpts: []QueryOption{{Name: "timeoutMs", Value: "1000"}},
			want: sqlRequest{
				Sql:          "/* clientQueryId: jdoe */\nSELECT 1",
				QueryOptions: "clientQueryId=jdoe;timeoutMs=1000",
			},
		},
		{
			name:      "query option wins",
			id:        "jdoe",
			queryOpts: []QueryOption{{Name: "clientQueryId", Value: "mine"}},
			want: sqlRequest{
				Sql:          "/* clientQueryId: jdoe */\nSELECT 1",
				QueryOptions: "clientQueryId=mine",
			},
		},
		{
			name:       "datasource option wins",
			id:         "jdoe",
			clientOpts: []QueryOption{{Name: "ClientQueryId", Value: "ds"}},
			want: sqlRequest{
				Sql:          "/* clientQueryId: jdoe */\nSELECT 1",
				QueryOptions: "ClientQueryId=ds",
			},
		},
		{name: "semicolon is not sent", id: "a;b", want: sqlRequest{Sql: "SELECT 1"}},
		{name: "equals is not sent", id: "a=b", want: sqlRequest{Sql: "SELECT 1"}},
		{name: "comment end is not sent", id: "a*/ DROP", want: sqlRequest{Sql: "SELECT 1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, got := captureBrokerClient(t, ClientProperties{QueryOptions: tt.clientOpts})
			query := NewSqlQuery("SELECT 1")
			query.QueryOptions = tt.queryOpts
			ctx := WithClientQueryId(context.Background(), func() string { return tt.id })
			_, err := client.ExecuteSqlQuery(ctx, query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, *got)
		})
	}

	t.Run("a new id for each query", func(t *testing.T) {
		client, got := captureBrokerClient(t, ClientProperties{})
		n := 0
		ctx := WithClientQueryId(context.Background(), func() string { n++; return fmt.Sprintf("q%d", n) })
		for _, want := range []string{"clientQueryId=q1", "clientQueryId=q2"} {
			_, err := client.ExecuteSqlQuery(ctx, NewSqlQuery("SELECT 1"))
			require.NoError(t, err)
			assert.Equal(t, want, got.QueryOptions)
		}
	})

	t.Run("render for display is never tagged", func(t *testing.T) {
		client, _ := captureBrokerClient(t, ClientProperties{})
		assert.Equal(t, "SELECT 1", client.RenderSql(NewSqlQuery("SELECT 1")))
	})
}
