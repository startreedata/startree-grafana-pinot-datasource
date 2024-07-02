package plugin

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPinotResourceHandler_distinctValues(t *testing.T) {
	pinotClient, err := NewPinotClient(PinotProperties{
		ControllerUrl: "https://pinot.demo.teprod.startree.cloud",
		BrokerUrl:     "https://broker.pinot.demo.teprod.startree.cloud",
		Authorization: "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjM3YjhiZjE2NmFhNDI1ZjBhYTg5NDU1MGFlOTI3ZTAwM2Q0YWNjYTYifQ.eyJpc3MiOiJodHRwczovL2lkZW50aXR5LmRlbW8udGVwcm9kLnN0YXJ0cmVlLmNsb3VkIiwic3ViIjoiQ2lObmIyOW5iR1V0YjJGMWRHZ3lmREV4TVRZeU1Ea3lOVEkyTmpZMU5EUXpNakF6T1JJa1ltWXdZekE0TkdZdFpUTTNNQzAwTmpNMExXRXhOREV0Wm1JME4yWXlNekZoWXpBMSIsImF1ZCI6Im1hbmFnZWQtcGlub3QiLCJleHAiOjE3MjAwMjQ0OTUsImlhdCI6MTcxOTkzODA5NSwibm9uY2UiOiJyYW5kb21fc3RyaW5nIiwiYXRfaGFzaCI6IkVrTkEyako0OEdaak04a3UxY084YVEiLCJlbWFpbCI6ImphY2tzb25Ac3RhcnRyZWUuYWkiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiZ3JvdXBzIjpbImFkbWluLW9yZy0yaGV3M2JoZWRoYTgiLCJzdGFydHJlZS1vcHMtZGVmYXVsdC1zcmUiXSwibmFtZSI6IkphY2tzb24gQXJnbyJ9.zEdWSjgCq_1mW8jTpPze28-TNDMOFi7KBPETD15g5ybCQc3Z5DvAAbYCDwVermC23-dd26FgGIa3f-gGO5q_TXXjtPCQg7k7_U6M2-unVcM6DeGfXrjTa1PhCZ4F8uNLOev7_6UmvZF8wHMBpszxsgRAW9rEVXBIXGK7Ke0DJ2xnjTALZw3uizzl2IJ4o8UoeygxX_RtKtspm8iZN819ZjnS3hr4n0lIc6L112SJ53mEdj-plnx0hEiDIyLXF8bxfEHWcOTr24GIewoFa_kXzLxFZtuLSRn5D_CaztX6dzsAfsngcKEeIUQKL0JqjRTYCG_SRwaHY5HlgdNZVLccVA",
	})
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc((&PinotResourceHandler{pinotClient}).distinctValues))
	defer server.Close()

	var payload bytes.Buffer
	require.NoError(t, json.NewEncoder(&payload).Encode(map[string]interface{}{
		"timeRange":    map[string]interface{}{"from": "2017-12-02T18:17:33.473Z", "to": "2018-04-14T14:02:31.973Z"},
		"databaseName": "default",
		"tableName":    "CleanLogisticData",
		"timeColumn":   "timestamp",
		"columnName":   "City",
	}))

	req, err := http.NewRequest(http.MethodPost, server.URL, &payload)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var respData map[string]interface{}
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&respData))

	print(respData)

	assert.Equal(t, []interface{}{"'Los_Angeles'", "'New_York'", "'San_Francisco'"}, respData["valueExprs"])
}
