package resources

import (
	"bytes"
	"encoding/json"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPinotResourceHandler_PreviewSqlBuilder(t *testing.T) {
	server := newTestServer(t)
	defer server.Close()

	want := strings.TrimSpace(`
SET timeoutMs=1;

SELECT
    DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '30:MINUTES') AS "time",
    MAX("value") AS "metric"
FROM
    "benchmark"
WHERE
    "ts" >= 1388327400000 AND "ts" < 1391281200000
GROUP BY
    "time"
ORDER BY
    "time" DESC,
    "fabric" DESC
LIMIT 100000;
`)

	var got map[string]interface{}
	doPostRequest(t, server.URL+"/preview/sql/builder", `{
  "aggregationFunction": "MAX",
  "intervalSize": "30m",
  "metricColumn": {"name":"value"},
  "tableName": "benchmark",
  "timeColumn": "ts",
  "timeRange": {
    "to": "2014-02-01T18:44:26.214Z",
    "from": "2013-12-29T14:50:28.931Z"
  },
  "limit": -1,
  "groupBy": ["fabric"],
  "orderBy": [{"columnName": "time", "direction": "DESC"}, {"columnName": "fabric", "direction": "DESC"}],
  "queryOptions": [{"name":"timeoutMs", "value":"1"}],
  "expandMacros": true
}`, &got)

	assert.Equal(t, want, got["result"])
}

func TestPinotResourceHandler_DistinctValues(t *testing.T) {
	server := newTestServer(t)
	defer server.Close()

	var got json.RawMessage
	doPostRequest(t, server.URL+"/query/distinctValues", `{
		"timeRange":    {"from": "2018-01-01T00:00:00Z", "to": "2024-12-31T00:00:00Z"},
		"tableName":    "benchmark",
		"timeColumn":   "ts",
		"columnName":   "fabric"
	}`, &got)

	want := `{"code":200,"result":["'fabric_0000'","'fabric_0001'","'fabric_0002'","'fabric_0003'","'fabric_0004'",
		"'fabric_0005'","'fabric_0006'","'fabric_0007'","'fabric_0008'","'fabric_0009'","'fabric_0010'","'fabric_0011'",
		"'fabric_0012'","'fabric_0013'","'fabric_0014'"]}`

	assertEqualJson(t, want, got)
}

func TestPinotResourceHandler_CodeSqlPreview(t *testing.T) {
	server := newTestServer(t)
	defer server.Close()

	var want = `SELECT 
   DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '30:MINUTES')  AS  "time" ,
  SUM("value") AS  "metric" 
FROM  "benchmark" 
WHERE  "ts" >= 1388327400000 AND "ts" < 1391281200000 
GROUP BY  DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '30:MINUTES') 
ORDER BY  "time"  DESC
LIMIT 1000000`

	code := `SELECT 
  $__timeGroup("ts") AS $__timeAlias(),
  SUM("value") AS $__metricAlias()
FROM $__table()
WHERE $__timeFilter("ts")
GROUP BY $__timeGroup("ts")
ORDER BY $__timeAlias() DESC
LIMIT 1000000`

	var data bytes.Buffer
	require.NoError(t, json.NewEncoder(&data).Encode(map[string]interface{}{
		"aggregationFunction": "MAX",
		"intervalSize":        "30m",
		"metricAlias":         "metric",
		"tableName":           "benchmark",
		"timeAlias":           "time",
		"timeRange": map[string]interface{}{
			"to":   "2014-02-01T18:44:26.214Z",
			"from": "2013-12-29T14:50:28.931Z",
		},
		"code": code,
	}))

	var got map[string]interface{}
	doPostRequest(t, server.URL+"/preview/sql/code", data.String(), &got)
	assert.Equal(t, want, got["result"])
}

func TestPinotResourceHandler_ListSuggestedGranularities(t *testing.T) {
	server := newTestServer(t)
	defer server.Close()

	testCases := []struct {
		payload string
		want    string
	}{
		{
			payload: `{"tableName":"derivedTimeBuckets","timeColumn":"ts"}`,
			want: `{
				"code":200,
				"result":[
					{"name":"auto","optimized":false,"seconds":0},
					{"name":"MILLISECONDS","optimized":true,"seconds":0.001},
					{"name":"SECONDS","optimized":false,"seconds":1},
					{"name":"MINUTES","optimized":true,"seconds":60},
					{"name":"2:MINUTES","optimized":true,"seconds":120},
					{"name":"5:MINUTES","optimized":true,"seconds":300},
					{"name":"10:MINUTES","optimized":true,"seconds":600},
					{"name":"15:MINUTES","optimized":true,"seconds":900},
					{"name":"30:MINUTES","optimized":true,"seconds":1800},
					{"name":"HOURS","optimized":true,"seconds":3600},
					{"name":"DAYS","optimized":true,"seconds":86400}
				]
			}`,
		}, {
			payload: `{"tableName":"hourlyEvents","timeColumn":"ts"}`,
			want: `{
				"code":200,
				"result":[
					{"name":"auto","optimized":false,"seconds":0},
					{"name":"HOURS","optimized":false,"seconds":3600},
					{"name":"DAYS","optimized":false,"seconds":86400}
				]
			}`,
		}, {
			payload: `{}`,
			want: `{
				"code":200,
				"result":[
					{"name":"auto","optimized":false,"seconds":0},
					{"name":"MILLISECONDS","optimized":false,"seconds":0.001},
					{"name":"SECONDS","optimized":false,"seconds":1},
					{"name":"MINUTES","optimized":false,"seconds":60},
					{"name":"HOURS","optimized":false,"seconds":3600},
					{"name":"DAYS","optimized":false,"seconds":86400}
				]
			}`,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.payload, func(t *testing.T) {
			var got json.RawMessage
			doPostRequest(t, server.URL+"/granularities", tt.payload, &got)
			assertEqualJson(t, tt.want, got)
		})
	}
}

func TestPinotResourceHandler_ListColumns(t *testing.T) {
	server := newTestServer(t)
	defer server.Close()
	testCases := []struct {
		name    string
		payload string
		want    string
	}{
		{
			name:    "table=None,ts=None",
			payload: `{}`,
			want:    `{"code":200}`,
		}, {
			name:    "table=benchmark,ts=None",
			payload: `{"tableName":"benchmark"}`,
			want: `{"code":200, "result":[
				{"name":"ts","dataType":"TIMESTAMP","isTime":true},
				{"name":"fabric","dataType":"STRING"},
				{"name":"pattern","dataType":"STRING"},
				{"name":"value","dataType":"DOUBLE","isMetric":true}
			]}`,
		}, {
			name:    "table=derivedTimeBuckets,ts=None",
			payload: `{"tableName":"derivedTimeBuckets"}`,
			want: `{"code":200,"result":[
				{"name":"ts","dataType":"TIMESTAMP","isTime":true},
				{"name":"ts_1m","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"ts_2m","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"ts_5m","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"ts_10m","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"ts_15m","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"ts_30m","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"ts_1h","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"ts_1d","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"value","dataType":"DOUBLE","isMetric":true}
			]}`,
		}, {
			name: "table=derivedTimeBuckets",
			payload: `{
				"timeRange":    {"from": "2018-01-01T00:00:00Z", "to": "2024-12-31T00:00:00Z"},
				"tableName":    "derivedTimeBuckets",
				"timeColumn":   "ts"
			}`,
			want: `{"code":200,"result":[
				{"name":"ts","dataType":"TIMESTAMP","isTime":true},
				{"name":"ts_1m","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"ts_2m","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"ts_5m","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"ts_10m","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"ts_15m","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"ts_30m","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"ts_1h","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"ts_1d","dataType":"TIMESTAMP","isTime":true,"isDerived":true},
				{"name":"value","dataType":"DOUBLE","isMetric":true}
			]}`,
		}, {
			name: "table=benchmark",
			payload: `{
				"timeRange":    {"from": "2018-01-01T00:00:00Z", "to": "2024-12-31T00:00:00Z"},
				"tableName":    "benchmark",
				"timeColumn":   "ts"
			}`,
			want: `{"code":200,"result":[
				{"name":"ts","dataType":"TIMESTAMP","isTime":true},
				{"name":"fabric","dataType":"STRING"},
				{"name":"pattern","dataType":"STRING"},
				{"name":"value","dataType":"DOUBLE","isMetric":true}
			]}`,
		}, {
			name: "table=timeSeriesWithMapLabels",
			payload: `{
				"timeRange":    {"from": "2018-01-01T00:00:00Z", "to": "2024-12-31T00:00:00Z"},
				"tableName":    "timeSeriesWithMapLabels",
				"timeColumn":   "ts"
			}`,
			want: `{"code":200,"result":[
				{"name":"ts","dataType":"TIMESTAMP","isTime":true},
				{"name":"metric","dataType":"STRING"},
				{"name":"value","dataType":"DOUBLE","isMetric":true},
				{"name":"labels","key":"db","dataType":"STRING"},
				{"name":"labels","key":"method","dataType":"STRING"},
				{"name":"labels","key":"path","dataType":"STRING"},
				{"name":"labels","key":"status","dataType":"STRING"},
				{"name":"labels","key":"table","dataType":"STRING"}
			]}`,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			var got json.RawMessage
			doPostRequest(t, server.URL+"/columns", tt.payload, &got)
			assertEqualJson(t, tt.want, got)
		})
	}
}

func newTestServer(t *testing.T) *httptest.Server {
	client := test_helpers.SetupPinotAndCreateClient(t)
	return httptest.NewServer(NewPinotResourceHandler(client))
}

func doPostRequest(t *testing.T, url string, data string, dest interface{}) {
	t.Helper()

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

func assertEqualJson[T1 string | []byte | json.RawMessage, T2 string | []byte | json.RawMessage](t *testing.T, want T1, got T2) {
	t.Helper()
	wantPretty, err := json.MarshalIndent(json.RawMessage(want), "", "  ")
	require.NoError(t, err, "want invalid json")
	gotPretty, err := json.MarshalIndent(json.RawMessage(got), "", "  ")
	require.NoError(t, err, "got invalid json")
	//assert.JSONEq(t, string(wantPretty), string(gotPretty))
	assert.Equal(t, string(wantPretty), string(gotPretty))
}
