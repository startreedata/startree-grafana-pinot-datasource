package pinot

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestPinotClient_ExecuteSqlQuery(t *testing.T) {
	ctx := context.Background()
	client := setupPinotAndCreateClient(t)
	query := NewSqlQuery(`
		SELECT fabric, "pattern", ts, value
		FROM benchmark
		ORDER BY ts, fabric, "pattern", value
		LIMIT 3`)
	resp, err := client.ExecuteSqlQuery(ctx, query)
	assert.NoError(t, err)
	assert.Empty(t, resp.Exceptions)
	assert.Equal(t, &ResultTable{
		DataSchema: DataSchema{
			ColumnDataTypes: []string{"STRING", "STRING", "TIMESTAMP", "DOUBLE"},
			ColumnNames:     []string{"fabric", "pattern", "ts", "value"}},
		Rows: [][]interface{}{
			{"fabric_0000", "pattern_0001", "2024-10-01 00:00:00.0", json.Number("-1.037174743344011")},
			{"fabric_0000", "pattern_0011", "2024-10-01 00:00:00.0", json.Number("101.49030354351736")},
			{"fabric_0000", "pattern_0012", "2024-10-01 00:00:00.0", json.Number("201.0248989609479")},
		},
	}, resp.ResultTable)
}

// blockingBrokerClient returns a client pointed at an httptest server that blocks until
// the request context is cancelled (or the test ends). started is closed once the handler
// is entered, so the caller knows the request is in-flight before cancelling.
func blockingBrokerClient(t *testing.T) (*Client, chan struct{}) {
	t.Helper()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		select {
		case <-r.Context().Done(): // client aborted (cancel or timeout)
		case <-release: // test ending — let the handler return so Close doesn't hang
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) }) // LIFO: runs before server.Close
	client := NewPinotClient(server.Client(), ClientProperties{BrokerUrl: server.URL})
	return client, started
}

func TestPinotClient_ExecuteSqlQuery_ContextCancelled(t *testing.T) {
	client, started := blockingBrokerClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()

	_, err := client.ExecuteSqlQuery(ctx, NewSqlQuery("SELECT 1"))
	assert.ErrorIs(t, err, context.Canceled)
}

func TestPinotClient_ExecuteSqlQuery_TimeoutDeadline(t *testing.T) {
	client, _ := blockingBrokerClient(t)
	client.properties.QueryTimeout = 50 * time.Millisecond

	_, err := client.ExecuteSqlQuery(context.Background(), NewSqlQuery("SELECT 1"))
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestApplyRowLimit(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		maxRowLimit int
		want        string
	}{
		{"disabled", "select * from t", 0, "select * from t"},
		{"appends when missing", "select * from t", 1000, "select * from t LIMIT 1000"},
		{"trims trailing semicolon", "select * from t;", 1000, "select * from t LIMIT 1000"},
		{"keeps explicit limit", "select * from t LIMIT 5", 1000, "select * from t LIMIT 5"},
		{"keeps explicit limit case-insensitive", "select * from t limit 5", 1000, "select * from t limit 5"},
		{"appends after order by", "select * from t ORDER BY a", 50, "select * from t ORDER BY a LIMIT 50"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, applyRowLimit(tt.sql, tt.maxRowLimit))
		})
	}
}

func TestPinotClient_RenderSql_AppliesRowLimit(t *testing.T) {
	client := NewPinotClient(http.DefaultClient, ClientProperties{MaxRowLimit: 1000})
	got := client.RenderSql(SqlQuery{Sql: "select * from benchmark"})
	assert.Equal(t, "select * from benchmark LIMIT 1000", got)
}

func TestNewBrokerExceptionError(t *testing.T) {
	got := NewBrokerExceptionError([]BrokerException{{Message: "this is a broker exception", ErrorCode: 1}})
	assert.Equal(t, &BrokerExceptionError{
		Exceptions: []BrokerException{{Message: "this is a broker exception", ErrorCode: 1}},
	}, got)
}

func TestBrokerExceptionError_Error(t *testing.T) {
	err := NewBrokerExceptionError([]BrokerException{{Message: "this is a broker exception", ErrorCode: 1}})
	assert.Equal(t, "Broker request completed with exceptions:\nCode 1: this is a broker exception", err.Error())
}

func TestPinotClient_RenderSql(t *testing.T) {
	client := NewPinotClient(http.DefaultClient, ClientProperties{
		QueryOptions: []QueryOption{
			{Name: "useMultistageEngine", Value: "true"},
			{Name: "timeoutMs", Value: "100"},
		},
	})

	query := SqlQuery{
		Sql: `select * from benchmark`,
		QueryOptions: []QueryOption{
			{Name: "enableNullHandling", Value: "true"},
			{Name: "maxExecutionThreads", Value: "5"},
		},
	}

	got := client.RenderSql(query)
	assert.Equal(t, `select * from benchmark;

SET enableNullHandling=true;
SET maxExecutionThreads=5;
SET useMultistageEngine=true;
SET timeoutMs=100;`, got)
}
