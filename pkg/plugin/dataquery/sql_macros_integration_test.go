package dataquery

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMacroExpansion_Integration verifies that macro-expanded SQL is accepted by a real Pinot
// broker and that the scalar time macros render to the expected literal values. Unit tests
// (TestExpandMacros) only assert the rendered string in isolation with a hand-built schema; this
// exercises the full path — schema/config fetch from the controller, expansion, and broker
// execution — so it catches SQL that our renderer produces but a real broker rejects.
func TestMacroExpansion_Integration(t *testing.T) {
	ctx := context.Background()
	client := test_helpers.SetupPinotAndCreateClient(t)

	// The benchmark table has a "ts" time column; its data lives in 2024-10-01 00:00–00:05, so this
	// range guarantees $__timeFilter("ts") matches at least one row.
	from := time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 10, 1, 0, 5, 0, 0, time.UTC)

	query := PinotQlCodeQuery{
		TableName:    "benchmark",
		DisplayType:  DisplayTypeTable,
		TimeRange:    TimeRange{From: from, To: to},
		IntervalSize: time.Minute,
		// Selecting a real column alongside the scalar macros keeps this a valid projection while
		// proving $__fromTime / $__toTime / $__interval_s render to broker-valid integer literals.
		// $__adHocFilter (no filters) expands to TRUE, exercising that macro on the broker too.
		Code: `SELECT
  "ts" AS "ts",
  $__fromTime AS "from_s",
  $__toTime AS "to_s",
  $__interval_s AS "interval_s"
FROM $__table()
WHERE $__timeFilter("ts") AND $__adHocFilter
LIMIT 1`,
	}

	// RenderSqlQuery builds the MacroEngine from the live schema/configs and expands every macro.
	rendered, err := query.RenderSqlQuery(ctx, client)
	require.NoError(t, err)

	resp, err := client.ExecuteSqlQuery(ctx, rendered)
	require.NoError(t, err)
	require.False(t, resp.HasExceptions(), "broker rejected macro-expanded SQL %q: %v", rendered.Sql, resp.Exceptions)
	require.True(t, resp.HasData(), "expected at least one row for the configured time range")

	row := resp.ResultTable.Rows[0]
	require.Len(t, row, 4)
	assert.EqualValues(t, from.Unix(), asInt64(t, row[1]), "$__fromTime should be the panel start in epoch seconds")
	assert.EqualValues(t, to.Unix(), asInt64(t, row[2]), "$__toTime should be the panel end in epoch seconds")
	assert.EqualValues(t, 60, asInt64(t, row[3]), "$__interval_s should be the interval (1m) in seconds")
}

// The broker client decodes with json.Number (UseNumber), so numeric cells arrive as json.Number;
// normalize to int64 for comparison.
func asInt64(t *testing.T, v interface{}) int64 {
	t.Helper()
	n, ok := v.(json.Number)
	require.True(t, ok, "expected a json.Number cell, got %T (%v)", v, v)
	i, err := n.Int64()
	require.NoError(t, err)
	return i
}
