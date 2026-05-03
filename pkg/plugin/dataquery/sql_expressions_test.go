package dataquery

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/stretchr/testify/assert"
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
}
