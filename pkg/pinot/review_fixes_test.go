package pinot

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// emptyLabelsClient returns a client for a time-series table whose label query returns no rows.
func emptyLabelsClient(t *testing.T) *Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/schema"):
			_, _ = w.Write([]byte(`{"schemaName":"ts","dimensionFieldSpecs":[{"name":"labels","dataType":"JSON"}]}`))
		case r.URL.Path == "/query/sql":
			_, _ = w.Write([]byte(`{"resultTable":{"dataSchema":{"columnNames":["labels"],"columnDataTypes":["JSON"]},"rows":[]},"exceptions":[]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return NewPinotClient(server.Client(), ClientProperties{ControllerUrl: server.URL, BrokerUrl: server.URL})
}

func TestPinotClient_TimeSeriesLabels_NoData(t *testing.T) {
	client := emptyLabelsClient(t)
	from, to := time.Unix(0, 0), time.Unix(3600, 0)

	names, err := client.ListTimeSeriesLabelNames(context.Background(), TimeSeriesLabelNamesQuery{
		TableName: "ts", MetricName: "m", From: from, To: to,
	})
	require.NoError(t, err)
	assert.Empty(t, names)

	values, err := client.ListTimeSeriesLabelValues(context.Background(), TimeSeriesLabelValuesQuery{
		TableName: "ts", MetricName: "m", LabelName: "host", From: from, To: to,
	})
	require.NoError(t, err)
	assert.Empty(t, values)
}

func TestLabelsCollection_NilSafe(t *testing.T) {
	var x *labelsCollection
	assert.Nil(t, x.names())
	assert.Nil(t, x.valuesOf("host"))
}

func TestTransportErrorsDoNotLeakURL(t *testing.T) {
	const secret = "s3cr3t-token"

	// Listen and close straight away so the request fails at the transport level.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	client := NewPinotClient(http.DefaultClient, ClientProperties{
		BrokerUrl: "http://admin:" + secret + "@" + addr + "/?token=" + secret,
	})
	_, err = client.ExecuteSqlQuery(context.Background(), NewSqlQuery("SELECT 1"))
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret)
	assert.NotContains(t, RedactError(err).Error(), secret)
	assert.Contains(t, err.Error(), "pinot/http: Request failed: POST")

	t.Run("context errors still unwrap", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := client.ExecuteSqlQuery(ctx, NewSqlQuery("SELECT 1"))
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.NotContains(t, err.Error(), secret)
	})
}

func TestRedactError_URLError(t *testing.T) {
	err := &url.Error{
		Op:  "Post",
		URL: "https://admin:s3cr3t@broker.example.com/query/sql?token=s3cr3t",
		Err: errors.New("dial tcp: connection refused"),
	}
	got := RedactError(err)
	assert.EqualError(t, got, "pinot/http: Request failed: Post dial tcp: connection refused")
	assert.NotContains(t, got.Error(), "s3cr3t")
}
