package dataquery

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilterExprsFrom(t *testing.T) {
	var filters []DimensionFilter
	assert.NoError(t, json.NewDecoder(strings.NewReader(`[
	  {
		"columnName": "AirlineID",
		"operator": "=",
		"valueExprs": [
		  "19393",
		  "19790"
		]
	  },
	  {
		"columnName": "ArrTime",
		"operator": ">",
		"valueExprs": [
		  "-2147483648"
		]
	  },
	  {
		"columnName": "Cancelled",
		"operator": "=",
		"valueExprs": [
		  "0"
		]
	  },
	  {
		"columnName": "Carrier",
		"operator": "like",
		"valueExprs": [
		  "'DL'"
		]
	  },
	  {
		"operator": "like",
		"valueExprs": [
		  "'DL'"
		]
	  },
	  {
		"columnName": "Carrier",
		"operator": "in",
		"valueExprs": [
		  "'DL'"
		]
	  },
	  {
		"columnName": "Carrier",
		"operator": "not in",
		"valueExprs": [
		  "'DL'"
		]
	  },
	  {
		"columnName": "Carrier",
		"operator": "invalid",
		"valueExprs": [
		  "'DL'"
		]
	  },
	  {}
	]`)).Decode(&filters))

	got := FilterExprsFrom(filters)
	assert.EqualValues(t, []pinot.SqlExpr{
		`("AirlineID" = 19393 OR "AirlineID" = 19790)`,
		`("ArrTime" > -2147483648)`,
		`("Cancelled" = 0)`,
		`("Carrier" like 'DL')`,
		`("Carrier" in ('DL'))`,
		`("Carrier" not in ('DL'))`,
	}, got)
}

func TestFilterExprsFrom_SubqueryExpr(t *testing.T) {
	t.Run("subqueryExpr with IN operator", func(t *testing.T) {
		filters := []DimensionFilter{
			{
				ColumnName:   "playerName",
				Operator:     "in",
				SubqueryExpr: "SELECT DISTINCT playerName FROM baseballStats",
			},
		}
		got := FilterExprsFrom(filters)
		assert.EqualValues(t, []pinot.SqlExpr{
			`("playerName" in (SELECT DISTINCT playerName FROM baseballStats))`,
		}, got)
	})

	t.Run("subqueryExpr with NOT IN operator", func(t *testing.T) {
		filters := []DimensionFilter{
			{
				ColumnName:   "playerName",
				Operator:     "not in",
				SubqueryExpr: "SELECT DISTINCT playerName FROM baseballStats",
			},
		}
		got := FilterExprsFrom(filters)
		assert.EqualValues(t, []pinot.SqlExpr{
			`("playerName" not in (SELECT DISTINCT playerName FROM baseballStats))`,
		}, got)
	})

	t.Run("subqueryExpr with != operator maps to NOT IN", func(t *testing.T) {
		filters := []DimensionFilter{
			{
				ColumnName:   "playerName",
				Operator:     "!=",
				SubqueryExpr: "SELECT DISTINCT playerName FROM baseballStats",
			},
		}
		got := FilterExprsFrom(filters)
		assert.EqualValues(t, []pinot.SqlExpr{
			`("playerName" not in (SELECT DISTINCT playerName FROM baseballStats))`,
		}, got)
	})

	t.Run("subqueryExpr with = operator maps to IN", func(t *testing.T) {
		filters := []DimensionFilter{
			{
				ColumnName:   "playerName",
				Operator:     "=",
				SubqueryExpr: "SELECT DISTINCT playerName FROM baseballStats",
			},
		}
		got := FilterExprsFrom(filters)
		assert.EqualValues(t, []pinot.SqlExpr{
			`("playerName" in (SELECT DISTINCT playerName FROM baseballStats))`,
		}, got)
	})

	t.Run("subqueryExpr with complex field key", func(t *testing.T) {
		filters := []DimensionFilter{
			{
				ColumnName:   "metadata",
				ColumnKey:    "region",
				Operator:     "in",
				SubqueryExpr: "SELECT DISTINCT region FROM regions",
			},
		}
		got := FilterExprsFrom(filters)
		assert.EqualValues(t, []pinot.SqlExpr{
			`("metadata"['region'] in (SELECT DISTINCT region FROM regions))`,
		}, got)
	})

	t.Run("mixed subqueryExpr and normal filters", func(t *testing.T) {
		filters := []DimensionFilter{
			{
				ColumnName: "status",
				Operator:   "=",
				ValueExprs: []string{"'active'"},
			},
			{
				ColumnName:   "playerName",
				Operator:     "in",
				SubqueryExpr: "SELECT DISTINCT playerName FROM baseballStats",
			},
		}
		got := FilterExprsFrom(filters)
		assert.EqualValues(t, []pinot.SqlExpr{
			`("status" = 'active')`,
			`("playerName" in (SELECT DISTINCT playerName FROM baseballStats))`,
		}, got)
	})

	t.Run("subqueryExpr with empty column or operator is skipped", func(t *testing.T) {
		// User is mid-edit: the filter has subqueryExpr (auto-injected by interpolateVariables)
		// but ColumnName/Operator aren't populated yet. Without this guard we'd render
		// `("" in (subquery))` and break SQL preview / resource requests.
		filters := []DimensionFilter{
			{
				Operator:     "in",
				SubqueryExpr: "SELECT DISTINCT entity FROM t",
			},
			{
				ColumnName:   "entity",
				SubqueryExpr: "SELECT DISTINCT entity FROM t",
			},
			{
				ColumnName:   "valid",
				Operator:     "in",
				SubqueryExpr: "SELECT DISTINCT entity FROM t",
			},
		}
		got := FilterExprsFrom(filters)
		assert.EqualValues(t, []pinot.SqlExpr{
			`("valid" in (SELECT DISTINCT entity FROM t))`,
		}, got)
	})
}

func TestExpandSubqueryMacros(t *testing.T) {
	ctx := context.Background()
	engine := MacroEngine{
		TableName: "highCardinality",
		TableSchema: pinot.TableSchema{
			DateTimeFieldSpecs: []pinot.DateTimeFieldSpec{{
				Name:     "ts",
				DataType: "LONG",
				Format:   "1:MILLISECONDS:EPOCH",
			}},
		},
		TimeRange: TimeRange{From: time.Unix(1, 0), To: time.Unix(2, 0)},
		TimeAlias: "__time",
	}

	t.Run("$__table() in SubqueryExpr is expanded to the configured table name", func(t *testing.T) {
		filters := []DimensionFilter{{
			ColumnName:   "entity",
			Operator:     "in",
			SubqueryExpr: `select Distinct entity from $__table() limit 4000`,
		}}
		got, err := ExpandSubqueryMacros(ctx, filters, engine)
		require.NoError(t, err)
		assert.Contains(t, got[0].SubqueryExpr, `"highCardinality"`)
		assert.NotContains(t, got[0].SubqueryExpr, "$__table")
	})

	t.Run("$__timeFilter inside SubqueryExpr is expanded to a real time predicate", func(t *testing.T) {
		filters := []DimensionFilter{{
			ColumnName:   "entity",
			Operator:     "in",
			SubqueryExpr: `SELECT DISTINCT entity FROM $__table() WHERE $__timeFilter("ts")`,
		}}
		got, err := ExpandSubqueryMacros(ctx, filters, engine)
		require.NoError(t, err)
		assert.Contains(t, got[0].SubqueryExpr, `"highCardinality"`)
		assert.Contains(t, got[0].SubqueryExpr, `"ts" >=`)
		assert.NotContains(t, got[0].SubqueryExpr, "$__table")
		assert.NotContains(t, got[0].SubqueryExpr, "$__timeFilter")
	})

	t.Run("filter without SubqueryExpr is passed through unchanged", func(t *testing.T) {
		filters := []DimensionFilter{{
			ColumnName: "status",
			Operator:   "=",
			ValueExprs: []string{"'active'"},
		}}
		got, err := ExpandSubqueryMacros(ctx, filters, engine)
		require.NoError(t, err)
		assert.Equal(t, filters, got)
	})

	t.Run("SubqueryExpr without macros is passed through unchanged", func(t *testing.T) {
		filters := []DimensionFilter{{
			ColumnName:   "entity",
			Operator:     "in",
			SubqueryExpr: `SELECT DISTINCT entity FROM "highCardinality" LIMIT 4000`,
		}}
		got, err := ExpandSubqueryMacros(ctx, filters, engine)
		require.NoError(t, err)
		assert.Equal(t, filters[0].SubqueryExpr, got[0].SubqueryExpr)
	})

	t.Run("mixed filters — only SubqueryExpr ones are expanded", func(t *testing.T) {
		filters := []DimensionFilter{
			{ColumnName: "status", Operator: "=", ValueExprs: []string{"'active'"}},
			{ColumnName: "entity", Operator: "in", SubqueryExpr: `select entity from $__table()`},
		}
		got, err := ExpandSubqueryMacros(ctx, filters, engine)
		require.NoError(t, err)
		// Filter without SubqueryExpr is untouched.
		assert.Equal(t, filters[0], got[0])
		// Filter with SubqueryExpr has $__table() expanded to the table name.
		assert.Contains(t, got[1].SubqueryExpr, `"highCardinality"`)
		assert.NotContains(t, got[1].SubqueryExpr, "$__table")
	})
}
