package plugin

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPinotResourceHandler_SqlPreview(t *testing.T) {
	pinotClient := newPinotClient(t)

	server := httptest.NewServer(http.HandlerFunc((&PinotResourceHandler{pinotClient}).SqlPreview))
	defer server.Close()

	want := strings.TrimSpace(`
SELECT
    DATETIMECONVERT("timestamp", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '2:HOURS') AS "time",
    COUNT("Driver_Request_API_Errors") AS "metric"
FROM
    "CleanLogisticData"
WHERE
    "timestamp" >= 1512238653 AND "timestamp" <= 1523714551
    AND ("City" = 'Los_Angeles' OR "City" = 'New_York')
GROUP BY
    DATETIMECONVERT("timestamp", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '2:HOURS')
ORDER BY "time" ASC
LIMIT 1000000
`)

	var got map[string]interface{}
	doPostRequest(t, server.URL, `{
		"aggregationFunction": "COUNT",
		"databaseName":        "default",
		"intervalSize":        "2h",
		"metricColumn":        "Driver_Request_API_Errors",
		"tableName":           "CleanLogisticData",
		"timeColumn":          "timestamp",
		"timeRange": {
			"to":   "2018-04-14T14:02:31.973Z",
			"from": "2017-12-02T18:17:33.473Z"
		},
		"filters": [{
			"columnName": "City",
			"operator":   "=",
			"valueExprs": ["'Los_Angeles'", "'New_York'"]
		}]
	}`, &got)

	assert.Equal(t, want, got["sql"])
}

func TestPinotResourceHandler_DistinctValues(t *testing.T) {
	pinotClient := newPinotClient(t)

	server := httptest.NewServer(http.HandlerFunc((&PinotResourceHandler{pinotClient}).DistinctValues))
	defer server.Close()

	var got json.RawMessage
	doPostRequest(t, server.URL, `{
		"timeRange":    {"from": "2017-12-02T18:17:33.473Z", "to": "2018-04-14T14:02:31.973Z"},
		"databaseName": "default",
		"tableName":    "CleanLogisticData",
		"timeColumn":   "timestamp",
		"columnName":   "City"
	}`, &got)

	want := `{"valueExprs":["'Los_Angeles'", "'New_York'", "'San_Francisco'"]}`

	assert.JSONEq(t, want, string(got))
}

func doPostRequest(t *testing.T, url string, data string, dest interface{}) {
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	var body bytes.Buffer
	_, err = body.ReadFrom(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "Response body: `%s`", body.String())

	require.NoError(t, json.NewDecoder(&body).Decode(dest))
}

func newPinotClient(t *testing.T) *PinotClient {
	pinotClient, err := NewPinotClient(PinotClientProperties{
		ControllerUrl: "https://pinot.demo.teprod.startree.cloud",
		BrokerUrl:     "https://broker.pinot.demo.teprod.startree.cloud",
		Authorization: "Basic YjBmZWI0YjcxN2UyNGE4M2E4NTE2OGRlMWMzODY3ODM6dnM3TkhjWjYrRTVFSXZ3OUpma0ZETnFtZmYrOTFZUk5NbHN1WkZucVVrMD0=",
	})
	require.NoError(t, err)
	return pinotClient
}
