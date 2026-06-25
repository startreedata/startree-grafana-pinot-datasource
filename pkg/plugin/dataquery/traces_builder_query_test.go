package dataquery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newTracesQuery() TracesBuilderQuery {
	return TracesBuilderQuery{
		TimeRange:          TimeRange{To: time.Unix(1, 0), From: time.Unix(0, 0)},
		TableName:          "otelTraces",
		TimeColumn:         "ts",
		TraceIdColumn:      ComplexField{Name: "trace_id"},
		SpanIdColumn:       ComplexField{Name: "span_id"},
		ParentSpanIdColumn: ComplexField{Name: "parent_span_id"},
		ServiceNameColumn:  ComplexField{Name: "service_name"},
		SpanNameColumn:     ComplexField{Name: "name"},
		DurationColumn:     ComplexField{Name: "duration_ns"},
		DurationUnit:       "ns",
		TagsColumn:         ComplexField{Name: "tags"},
	}
}

func TestTracesBuilderQuery_Validate(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		assert.NoError(t, newTracesQuery().Validate())
	})
	t.Run("optional columns omitted is valid", func(t *testing.T) {
		query := newTracesQuery()
		query.ParentSpanIdColumn = ComplexField{}
		query.ServiceNameColumn = ComplexField{}
		query.SpanNameColumn = ComplexField{}
		query.TagsColumn = ComplexField{}
		query.StatusColumn = ComplexField{}
		assert.NoError(t, query.Validate())
	})
	t.Run("no table name", func(t *testing.T) {
		query := newTracesQuery()
		query.TableName = ""
		assert.ErrorContains(t, query.Validate(), "table name is required")
	})
	t.Run("no time column", func(t *testing.T) {
		query := newTracesQuery()
		query.TimeColumn = ""
		assert.ErrorContains(t, query.Validate(), "time column is required")
	})
	t.Run("no trace id column", func(t *testing.T) {
		query := newTracesQuery()
		query.TraceIdColumn = ComplexField{}
		assert.ErrorContains(t, query.Validate(), "trace ID column is required")
	})
	t.Run("no span id column", func(t *testing.T) {
		query := newTracesQuery()
		query.SpanIdColumn = ComplexField{}
		assert.ErrorContains(t, query.Validate(), "span ID column is required")
	})
	t.Run("no duration column", func(t *testing.T) {
		query := newTracesQuery()
		query.DurationColumn = ComplexField{}
		assert.ErrorContains(t, query.Validate(), "duration column is required")
	})
}

func TestTracesBuilderQuery_durationFactor(t *testing.T) {
	cases := map[string]float64{
		"ns": 1e-6,
		"us": 1e-3,
		"µs": 1e-3,
		"ms": 1,
		"s":  1e3,
		"":   1,
	}
	for unit, want := range cases {
		t.Run(unit, func(t *testing.T) {
			query := newTracesQuery()
			query.DurationUnit = unit
			assert.Equal(t, want, query.durationFactor())
		})
	}
}

func TestTracesBuilderQuery_RenderSqlWithMacros_search(t *testing.T) {
	query := newTracesQuery()
	query.DimensionFilters = []DimensionFilter{{
		ColumnName: "service_name",
		ValueExprs: []string{"'frontend'"},
		Operator:   "=",
	}}
	query.QueryOptions = []QueryOption{{Name: "timeoutMs", Value: "1000"}}

	want := `SELECT
    "trace_id" AS 'traceID',
    "span_id" AS 'spanID',
    "ts" AS 'startTime',
    "duration_ns" AS 'duration',
    "parent_span_id" AS 'parentSpanID',
    "service_name" AS 'serviceName',
    "name" AS 'operationName',
    "tags" AS 'tags'
FROM $__table()
WHERE $__timeFilter("ts")
    AND ("service_name" = 'frontend')
ORDER BY "ts" DESC
LIMIT 100000;

SET timeoutMs=1000;`

	got, err := query.RenderSqlWithMacros()
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestTracesBuilderQuery_RenderSqlWithMacros_findByID(t *testing.T) {
	query := newTracesQuery()
	query.TraceId = "abc123"

	want := `SELECT
    "trace_id" AS 'traceID',
    "span_id" AS 'spanID',
    "ts" AS 'startTime',
    "duration_ns" AS 'duration',
    "parent_span_id" AS 'parentSpanID',
    "service_name" AS 'serviceName',
    "name" AS 'operationName',
    "tags" AS 'tags'
FROM $__table()
WHERE $__timeFilter("ts")
    AND ("trace_id" = 'abc123')
ORDER BY "ts" ASC
LIMIT 100000;`

	got, err := query.RenderSqlWithMacros()
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestTracesBuilderQuery_RenderSqlWithMacros_escapesTraceId(t *testing.T) {
	query := newTracesQuery()
	query.TraceId = "a'b" // a single quote must not break out of the string literal

	got, err := query.RenderSqlWithMacros()
	assert.NoError(t, err)
	assert.Contains(t, got, `AND ("trace_id" = 'a''b')`)
}
