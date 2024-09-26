package dataquery

import (
	"bytes"
	"context"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
	"time"
)

func TestR(t *testing.T) {
	want := make(url.Values)
	want.Add("start", "1727271155")
	want.Add("language", "promql")
	want.Add("query", "http_in_flight_requests")
	want.Add("end", "1727385162")
	want.Add("step", "15")
	want.Add("table", "prometheusMsg_REALTIME")

	req := &pinotlib.PinotTimeSeriesQuery{
		Query: "http_in_flight_requests",
		Start: time.Unix(1727271155, 0),
		End:   time.Unix(1727385162, 0),
		Step:  15 * time.Second,
		Table: "prometheusMsg_REALTIME",
	}

	assert.Equal(t, want, req.ToUrlValues())
}

func TestQ(t *testing.T) {
	client := test_helpers.NewPinotTestClient(t)
	resp, err := client.ExecuteTimeSeriesQuery(context.Background(), &pinotlib.PinotTimeSeriesQuery{
		Query: "http_in_flight_requests",
		Start: time.Unix(1727346410, 0),
		End:   time.Unix(1727389610, 0),
		Step:  15 * time.Second,
		Table: "prometheusMsg_REALTIME",
	})

	require.NotNil(t, resp)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	var buf bytes.Buffer
	buf.ReadFrom(resp.Body)
	assert.Equal(t, "", buf.String())

}
