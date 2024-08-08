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
	server := httptest.NewServer(NewPinotResourceHandler(newPinotTestClient(t)))
	defer server.Close()

	want := strings.TrimSpace(`
SET timeoutMs=1;

SELECT
    DATETIMECONVERT("ts", '1:MILLISECONDS:TIMESTAMP', '1:MILLISECONDS:EPOCH', '30:MINUTES') AS "time",
    MAX("AirTime") AS "metric"
FROM
    "airlineStats"
WHERE
    "ts" >= 1388328628931 AND "ts" <= 1391280266214
GROUP BY
    DATETIMECONVERT("ts", '1:MILLISECONDS:TIMESTAMP', '1:MILLISECONDS:EPOCH', '30:MINUTES')
ORDER BY
    "time" DESC,
    "metric" DESC
LIMIT 100000;
`)

	var got map[string]interface{}
	doPostRequest(t, server.URL+"/preview", `{
  "aggregationFunction": "MAX",
  "databaseName": "default",
  "intervalSize": "30m",
  "metricColumn": "AirTime",
  "tableName": "airlineStats",
  "timeColumn": "ts",
  "timeRange": {
    "to": "2014-02-01T18:44:26.214Z",
    "from": "2013-12-29T14:50:28.931Z"
  },
  "limit": -1,
  "orderBy": [
    {"columnName": "time", "direction": "DESC"},
    {"columnName": "metric", "direction": "DESC"}
  ],
  "queryOptions": [{"name":"timeoutMs", "value":"1"}]
}`, &got)

	assert.Equal(t, want, got["sql"])
}

func TestPinotResourceHandler_DistinctValues(t *testing.T) {
	server := httptest.NewServer(NewPinotResourceHandler(newPinotTestClient(t)))
	defer server.Close()

	var got json.RawMessage
	doPostRequest(t, server.URL+"/distinctValues", `{
		"timeRange":    {"from": "2018-01-01T00:00:00Z", "to": "2018-02-01T00:00:00Z"},
		"databaseName": "default",
		"tableName":    "githubEvents",
		"timeColumn":   "created_at_timestamp",
		"columnName":   "type"
	}`, &got)

	want := `{"valueExprs":["'CommitCommentEvent'", "'CreateEvent'", "'DeleteEvent'", "'ForkEvent'", "'GollumEvent'", 
				"'IssueCommentEvent'", "'IssuesEvent'", "'MemberEvent'", "'PublicEvent'", "'PullRequestEvent'"]}`

	assert.JSONEq(t, want, string(got))
}

func TestPinotResourceHandler_CodeSqlPreview(t *testing.T) {
	server := httptest.NewServer(NewPinotResourceHandler(newPinotTestClient(t)))
	defer server.Close()

	var want = `SELECT 
   DATETIMECONVERT("ts", '1:MILLISECONDS:TIMESTAMP', '1:MILLISECONDS:EPOCH', '30:MINUTES')  AS  "time" ,
  SUM("AirTime") AS  "metric" 
FROM  "airlineStats" 
WHERE  "ts" >= 1388328628931 AND "ts" <= 1391280266214 
GROUP BY  DATETIMECONVERT("ts", '1:MILLISECONDS:TIMESTAMP', '1:MILLISECONDS:EPOCH', '30:MINUTES') 
ORDER BY  "time"  DESC
LIMIT 1000000`

	code := `SELECT 
  $__timeGroup("ts") AS $__timeAlias(),
  SUM("AirTime") AS $__metricAlias()
FROM $__table()
WHERE $__timeFilter("ts")
GROUP BY $__timeGroup("ts")
ORDER BY $__timeAlias() DESC
LIMIT 1000000`

	var data bytes.Buffer
	require.NoError(t, json.NewEncoder(&data).Encode(map[string]interface{}{
		"aggregationFunction": "MAX",
		"databaseName":        "default",
		"intervalSize":        "30m",
		"metricAlias":         "AirTime",
		"tableName":           "airlineStats",
		"timeAlias":           "time",
		"timeRange": map[string]interface{}{
			"to":   "2014-02-01T18:44:26.214Z",
			"from": "2013-12-29T14:50:28.931Z",
		},
		"code": code,
	}))

	var got map[string]interface{}
	doPostRequest(t, server.URL+"/codePreview", data.String(), &got)
	assert.Equal(t, want, got["sql"])
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
