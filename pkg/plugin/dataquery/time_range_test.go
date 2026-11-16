package dataquery

// DP-3035: the dashboard time range must reach every Pinot query the plugin sends.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const timeRangeTestSchema = `{
  "schemaName": "events",
  "dimensionFieldSpecs": [{"name": "pt", "dataType": "STRING"}, {"name": "ts_sec", "dataType": "LONG"}],
  "metricFieldSpecs": [{"name": "attempt", "dataType": "LONG"}],
  "dateTimeFieldSpecs": [{"name": "timestamp", "dataType": "LONG", "format": "1:MILLISECONDS:EPOCH", "granularity": "1:MINUTES"}]
}`

const timeRangeTestTableConfig = `{"OFFLINE": {"tableName": "events_OFFLINE", "tableType": "OFFLINE",
  "segmentsConfig": {"timeColumnName": "timestamp"}}}`

// fakePinot serves a schema, table config and table metadata, answers every SQL query with a
// one-row result, and records the SQL and timeseries requests it receives.
type fakePinot struct {
	mu               sync.Mutex
	sql              []string
	timeseriesParams url.Values
	timeseriesBody   string
}

func (f *fakePinot) client(t *testing.T) *pinot.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/schema"):
			_, _ = io.WriteString(w, timeRangeTestSchema)
		case strings.HasSuffix(r.URL.Path, "/metadata"):
			_, _ = io.WriteString(w, `{"tableName": "events_OFFLINE"}`)
		case strings.HasPrefix(r.URL.Path, "/tables/"):
			_, _ = io.WriteString(w, timeRangeTestTableConfig)
		case r.URL.Path == "/query/sql":
			var body struct {
				Sql string `json:"sql"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.mu.Lock()
			f.sql = append(f.sql, body.Sql)
			f.mu.Unlock()
			_, _ = io.WriteString(w, `{"resultTable": {"dataSchema": {"columnNames": ["v"], "columnDataTypes": ["STRING"]}, "rows": [["x"]]}}`)
		case strings.HasSuffix(r.URL.Path, "/query_range"):
			f.mu.Lock()
			f.timeseriesParams = r.URL.Query()
			f.mu.Unlock()
			_, _ = io.WriteString(w, f.timeseriesBody)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return pinot.NewPinotClient(server.Client(), pinot.ClientProperties{ControllerUrl: server.URL, BrokerUrl: server.URL})
}

func (f *fakePinot) lastSql(t *testing.T) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	require.NotEmpty(t, f.sql, "no SQL was sent to the broker")
	return f.sql[len(f.sql)-1]
}

var testTimeRange = TimeRange{From: time.UnixMilli(1_700_000_040_000), To: time.UnixMilli(1_700_003_640_000)}

func noticeTexts(frames data.Frames) []string {
	var texts []string
	for _, frame := range frames {
		if frame.Meta == nil {
			continue
		}
		for _, n := range frame.Meta.Notices {
			texts = append(texts, n.Text)
		}
	}
	return texts
}

func TestPinotQlCodeQuery_TimeRangeNotices(t *testing.T) {
	ptr := func(b bool) *bool { return &b }
	tests := []struct {
		name          string
		code          string
		usesTimeRange *bool
		wantNotices   []string
	}{
		{name: "hard-coded literals warn", code: `SELECT COUNT(*) FROM events WHERE "timestamp" >= 1 AND "timestamp" < 2`,
			usesTimeRange: ptr(false), wantNotices: []string{NoTimeRangeWarning}},
		{name: "time macro does not warn", code: `SELECT COUNT(*) FROM events WHERE $__timeFilter`,
			usesTimeRange: ptr(true)},
		{name: "unknown (alerting/API callers) does not warn", code: `SELECT COUNT(*) FROM events`},
		{name: "numeric non-datetime column warns about the millis assumption",
			code: `SELECT COUNT(*) FROM events WHERE $__timeFilter(ts_sec)`, usesTimeRange: ptr(true),
			wantNotices: []string{"Column `ts_sec` is not a date time column, so the time filter assumes it holds epoch milliseconds. " +
				"If it holds epoch seconds, filter with `ts_sec >= $__fromTime AND ts_sec < $__toTime` instead, " +
				"or declare it in the schema's dateTimeFieldSpecs."}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakePinot{}
			resp := PinotQlCodeQuery{Code: tt.code, TableName: "events", TimeRange: testTimeRange,
				IntervalSize: time.Minute, DisplayType: DisplayTypeTable, UsesTimeRange: tt.usesTimeRange,
			}.Execute(fake.client(t), context.Background())
			require.NoError(t, resp.Error)
			assert.Equal(t, tt.wantNotices, noticeTexts(resp.Frames))
		})
	}
}

func TestPinotQlCodeQuery_NoArgTimeFilterUsesTableTimeColumn(t *testing.T) {
	fake := &fakePinot{}
	resp := PinotQlCodeQuery{Code: `SELECT COUNT(*) FROM events WHERE $__timeFilter`, TableName: "events",
		TimeRange: testTimeRange, IntervalSize: time.Minute, DisplayType: DisplayTypeTable,
	}.Execute(fake.client(t), context.Background())
	require.NoError(t, resp.Error)
	assert.Contains(t, fake.lastSql(t), `"timestamp" >= 1700000040000 AND "timestamp" < 1700003640000`)
}

func TestVariableQuery_TimeRange(t *testing.T) {
	t.Run("PINOT_QL_CODE expands time macros", func(t *testing.T) {
		fake := &fakePinot{}
		resp := VariableQuery{VariableType: VariableQueryTypePinotQlCode, TableName: "events",
			PinotQlCode: `SELECT DISTINCT pt FROM $__table WHERE $__timeFilter("timestamp")`,
			TimeRange:   testTimeRange, IntervalSize: time.Minute,
		}.Execute(fake.client(t), context.Background())
		require.NoError(t, resp.Error)
		assert.Equal(t, `SELECT DISTINCT pt FROM  "events"  WHERE  "timestamp" >= 1700000040000 AND "timestamp" < 1700003640000`,
			fake.lastSql(t))
	})

	// Distinct values stay unfiltered even with a time range, so tables without recent data keep their values.
	t.Run("DISTINCT_VALUES is not time-filtered", func(t *testing.T) {
		fake := &fakePinot{}
		resp := VariableQuery{VariableType: VariableQueryTypeDistinctValues, TableName: "events", ColumnName: "pt",
			TimeRange: testTimeRange,
		}.Execute(fake.client(t), context.Background())
		require.NoError(t, resp.Error)
		assert.NotContains(t, fake.lastSql(t), `"timestamp"`)
	})
}

func TestPromQlQuery_SendsDashboardTimeRange(t *testing.T) {
	from := time.Unix(1_700_000_037, 0) // Not step-aligned on purpose.
	to := time.Unix(1_700_003_637, 0)

	tests := []struct {
		name          string
		interval      time.Duration
		maxDataPoints int64
		wantStart     string
		wantStep      string
	}{
		{name: "grafana interval", interval: 30 * time.Second, maxDataPoints: 1000, wantStart: "1700000010", wantStep: "30"},
		{name: "zero interval derives step from maxDataPoints", interval: 0, maxDataPoints: 100, wantStart: "1700000028", wantStep: "36"},
		{name: "interval below the maxDataPoints bound is raised", interval: time.Second, maxDataPoints: 60, wantStart: "1699999980", wantStep: "60"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakePinot{timeseriesBody: `{"status": "success", "data": {"resultType": "matrix", "result": [
				{"metric": {"__name__": "m"}, "values": [[1700000040, "1"], [1700000070.5, "2"]]}]}}`}
			resp := PromQlQuery{TableName: "events", PromQlCode: "m", TimeRange: TimeRange{From: from, To: to},
				IntervalSize: tt.interval, MaxDataPoints: tt.maxDataPoints,
			}.Execute(fake.client(t), context.Background())
			require.NoError(t, resp.Error)

			assert.Equal(t, tt.wantStart, fake.timeseriesParams.Get("start"))
			assert.Equal(t, "1700003637", fake.timeseriesParams.Get("end"))
			assert.Equal(t, tt.wantStep, fake.timeseriesParams.Get("step"))
			require.Len(t, resp.Frames, 1)
			assert.Equal(t, 2, resp.Frames[0].Rows(), "fractional-second timestamps must parse")
		})
	}
}
