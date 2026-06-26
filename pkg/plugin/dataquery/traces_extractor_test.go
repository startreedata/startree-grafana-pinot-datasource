package dataquery

import (
	"encoding/json"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// num is a tiny helper for the json.Number values Pinot returns for numeric columns.
func num(s string) json.Number { return json.Number(s) }

// TestBuildTracesDataFrame_fullWithLink covers the "find trace by ID" shape: every span column is
// mapped, durations arrive in nanoseconds (scaled to ms), tags decode into key/value pairs, and a
// trace-to-logs data link is attached to the traceID field.
func TestBuildTracesDataFrame_fullWithLink(t *testing.T) {
	results := &pinot.ResultTable{
		DataSchema: pinot.DataSchema{
			ColumnNames: []string{
				TraceFieldTraceID, TraceFieldSpanID, TraceFieldStartTime, TraceFieldDuration,
				TraceFieldParentSpanID, TraceFieldServiceName, TraceFieldOperationName, TraceFieldStatusCode, TraceFieldTags,
			},
			ColumnDataTypes: []string{"STRING", "STRING", "LONG", "LONG", "STRING", "STRING", "STRING", "STRING", "JSON"},
		},
		Rows: [][]interface{}{
			{"t1", "s1", num("1700000000000"), num("5000000"), "", "frontend", "GET /", "STATUS_CODE_OK", `{"http.method":"GET","http.status":"200"}`},
			{"t1", "s2", num("1700000000500"), num("2500000"), "s1", "backend", "db.query", "STATUS_CODE_OK", `{"db.system":"pinot"}`},
		},
	}

	link := &TraceToLogsLink{
		DatasourceUID:  "ds-uid",
		DatasourceName: "Pinot",
		LogsTable:      "otelLogs",
		TraceIdColumn:  "trace_id",
		TimeColumn:     "ts",
		LogColumn:      "message",
	}

	got, err := BuildTracesDataFrame(results, pinot.DateTimeFormatMillisecondsEpoch(), 1e-6, link)
	require.NoError(t, err)

	traceIDField := data.NewField(TraceFieldTraceID, nil, []string{"t1", "t1"})
	traceIDField.Config = &data.FieldConfig{Links: []data.DataLink{{
		Title: "Logs for this trace",
		Internal: &data.InternalDataLink{
			DatasourceUID:  "ds-uid",
			DatasourceName: "Pinot",
			Query: map[string]any{
				"queryType":   "PinotQL",
				"editorMode":  "Builder",
				"displayType": "LOGS",
				"tableName":   "otelLogs",
				"timeColumn":  "ts",
				"logColumn":   map[string]any{"name": "message"},
				"filters": []map[string]any{{
					"columnName": "trace_id",
					"operator":   "=",
					"valueExprs": []string{"'${__value.raw}'"},
				}},
			},
		},
	}}}

	want := data.NewFrame("traces",
		traceIDField,
		data.NewField(TraceFieldSpanID, nil, []string{"s1", "s2"}),
		data.NewField(TraceFieldStartTime, nil, []float64{1700000000000, 1700000000500}),
		data.NewField(TraceFieldDuration, nil, []float64{5, 2.5}),
		data.NewField(TraceFieldParentSpanID, nil, []string{"", "s1"}),
		data.NewField(TraceFieldServiceName, nil, []string{"frontend", "backend"}),
		data.NewField(TraceFieldOperationName, nil, []string{"GET /", "db.query"}),
		// Status is STATUS_CODE_OK for both spans, so no standalone statusCode field is emitted and
		// the tags are unchanged (error tags are injected only for error spans).
		data.NewField(TraceFieldTags, nil, []json.RawMessage{
			json.RawMessage(`[{"key":"http.method","value":"GET"},{"key":"http.status","value":"200"}]`),
			json.RawMessage(`[{"key":"db.system","value":"pinot"}]`),
		}),
	)
	want.Meta = &data.FrameMeta{PreferredVisualization: data.VisTypeTrace}

	assert.Equal(t, want, got)
}

// TestBuildTracesDataFrame_minimalNoLink covers the "search" path with only the required columns
// mapped and no trace-to-logs configured: no optional fields, no tags, and no link on traceID.
func TestBuildTracesDataFrame_minimalNoLink(t *testing.T) {
	results := &pinot.ResultTable{
		DataSchema: pinot.DataSchema{
			ColumnNames:     []string{TraceFieldTraceID, TraceFieldSpanID, TraceFieldStartTime, TraceFieldDuration},
			ColumnDataTypes: []string{"STRING", "STRING", "LONG", "DOUBLE"},
		},
		Rows: [][]interface{}{
			{"t1", "s1", num("1700000000000"), num("12.5")},
			{"t2", "s2", num("1700000001000"), num("3")},
		},
	}

	got, err := BuildTracesDataFrame(results, pinot.DateTimeFormatMillisecondsEpoch(), 1, nil)
	require.NoError(t, err)

	want := data.NewFrame("traces",
		data.NewField(TraceFieldTraceID, nil, []string{"t1", "t2"}),
		data.NewField(TraceFieldSpanID, nil, []string{"s1", "s2"}),
		data.NewField(TraceFieldStartTime, nil, []float64{1700000000000, 1700000001000}),
		data.NewField(TraceFieldDuration, nil, []float64{12.5, 3}),
	)
	want.Meta = &data.FrameMeta{PreferredVisualization: data.VisTypeTrace}

	assert.Equal(t, want, got)
	// No data link when trace-to-logs is unconfigured.
	assert.Nil(t, got.Fields[0].Config)
}

// TestBuildTracesDataFrame_errorStatusFoldsIntoTags verifies that a mapped Status column does not
// produce a standalone field (Grafana ignores it) but instead contributes `error`/`otel.status_code`
// tags on error spans, leaving non-error spans' tags untouched.
func TestBuildTracesDataFrame_errorStatusFoldsIntoTags(t *testing.T) {
	results := &pinot.ResultTable{
		DataSchema: pinot.DataSchema{
			ColumnNames: []string{
				TraceFieldTraceID, TraceFieldSpanID, TraceFieldStartTime, TraceFieldDuration,
				TraceFieldStatusCode, TraceFieldTags,
			},
			ColumnDataTypes: []string{"STRING", "STRING", "LONG", "LONG", "STRING", "JSON"},
		},
		Rows: [][]interface{}{
			{"t1", "s1", num("1700000000000"), num("1"), "STATUS_CODE_ERROR", `{"http.method":"GET"}`},
			{"t1", "s2", num("1700000000500"), num("1"), "STATUS_CODE_OK", `{"db.system":"pinot"}`},
		},
	}

	got, err := BuildTracesDataFrame(results, pinot.DateTimeFormatMillisecondsEpoch(), 1, nil)
	require.NoError(t, err)

	// No statusCode field is emitted.
	for _, f := range got.Fields {
		assert.NotEqual(t, TraceFieldStatusCode, f.Name, "statusCode must not be a standalone field")
	}

	tags := fieldByName(t, got, TraceFieldTags)
	require.Equal(t, 2, tags.Len())
	// Error span: existing tags plus error=true and otel.status_code.
	assert.JSONEq(t,
		`[{"key":"http.method","value":"GET"},{"key":"error","value":true},{"key":"otel.status_code","value":"STATUS_CODE_ERROR"}]`,
		string(tags.At(0).(json.RawMessage)))
	// Non-error span: unchanged.
	assert.JSONEq(t, `[{"key":"db.system","value":"pinot"}]`, string(tags.At(1).(json.RawMessage)))
}

// TestBuildTracesDataFrame_errorStatusWithoutTagsColumn verifies the error fold-in still works when
// no tags column is mapped: a tags field is synthesized, error spans carry the error tags, and
// non-error spans get an empty array.
func TestBuildTracesDataFrame_errorStatusWithoutTagsColumn(t *testing.T) {
	results := &pinot.ResultTable{
		DataSchema: pinot.DataSchema{
			ColumnNames:     []string{TraceFieldTraceID, TraceFieldSpanID, TraceFieldStartTime, TraceFieldDuration, TraceFieldStatusCode},
			ColumnDataTypes: []string{"STRING", "STRING", "LONG", "LONG", "STRING"},
		},
		Rows: [][]interface{}{
			{"t1", "s1", num("1700000000000"), num("1"), "ERROR"},
			{"t1", "s2", num("1700000000500"), num("1"), "OK"},
		},
	}

	got, err := BuildTracesDataFrame(results, pinot.DateTimeFormatMillisecondsEpoch(), 1, nil)
	require.NoError(t, err)

	tags := fieldByName(t, got, TraceFieldTags)
	require.Equal(t, 2, tags.Len())
	assert.JSONEq(t, `[{"key":"error","value":true},{"key":"otel.status_code","value":"ERROR"}]`, string(tags.At(0).(json.RawMessage)))
	assert.JSONEq(t, `[]`, string(tags.At(1).(json.RawMessage)))
}

// TestBuildTracesDataFrame_errorStatusUpsertsExistingTags verifies the status fold-in is an upsert:
// when the source tags already carry `error` / `otel.status_code`, those entries are replaced (not
// duplicated) so the status-derived values are authoritative.
func TestBuildTracesDataFrame_errorStatusUpsertsExistingTags(t *testing.T) {
	results := &pinot.ResultTable{
		DataSchema: pinot.DataSchema{
			ColumnNames: []string{
				TraceFieldTraceID, TraceFieldSpanID, TraceFieldStartTime, TraceFieldDuration,
				TraceFieldStatusCode, TraceFieldTags,
			},
			ColumnDataTypes: []string{"STRING", "STRING", "LONG", "LONG", "STRING", "JSON"},
		},
		Rows: [][]interface{}{
			{"t1", "s1", num("1700000000000"), num("1"), "STATUS_CODE_ERROR",
				`{"error":"false","http.method":"GET","otel.status_code":"STATUS_CODE_UNSET"}`},
		},
	}

	got, err := BuildTracesDataFrame(results, pinot.DateTimeFormatMillisecondsEpoch(), 1, nil)
	require.NoError(t, err)

	tags := fieldByName(t, got, TraceFieldTags)
	require.Equal(t, 1, tags.Len())
	// The stale error/otel.status_code entries are dropped; http.method is kept; the authoritative
	// values are appended exactly once.
	assert.JSONEq(t,
		`[{"key":"http.method","value":"GET"},{"key":"error","value":true},{"key":"otel.status_code","value":"STATUS_CODE_ERROR"}]`,
		string(tags.At(0).(json.RawMessage)))
}

func fieldByName(t *testing.T, frame *data.Frame, name string) *data.Field {
	t.Helper()
	for _, f := range frame.Fields {
		if f.Name == name {
			return f
		}
	}
	require.Failf(t, "missing field", "frame has no field %q", name)
	return nil
}

func TestBuildTracesDataFrame_missingRequiredColumn(t *testing.T) {
	results := &pinot.ResultTable{
		DataSchema: pinot.DataSchema{
			ColumnNames:     []string{TraceFieldTraceID, TraceFieldSpanID, TraceFieldStartTime},
			ColumnDataTypes: []string{"STRING", "STRING", "LONG"},
		},
		Rows: [][]interface{}{{"t1", "s1", num("1700000000000")}},
	}

	_, err := BuildTracesDataFrame(results, pinot.DateTimeFormatMillisecondsEpoch(), 1, nil)
	assert.ErrorContains(t, err, "duration")
}
