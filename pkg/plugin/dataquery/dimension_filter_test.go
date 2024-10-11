package dataquery

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestFilterExprsFrom(t *testing.T) {
	want := []string{
		`("AirlineID" = 19393 OR "AirlineID" = 19790)`,
		`("ArrTime" > -2147483648)`,
		`("Cancelled" = 0)`,
		`("Carrier" like 'DL')`,
	}

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
	  {}
	]`)).Decode(&filters))

	got := FilterExprsFrom(filters)
	assert.EqualValues(t, want, got)
}

func TestDimensionFilter(t *testing.T) {
	testArgs := []struct {
		operator string
		expected string
	}{
		{FilterOpEquals, `("dim" = 'val1' OR "dim" = 'val2')`},
		{FilterOpNotEquals, `("dim" != 'val1' OR "dim" != 'val2')`},
		{FilterOpContains, `("dim" contains 'val1' OR "dim" contains 'val2')`},
		{FilterOpNotContains, `(not "dim" contains 'val1' OR not "dim" contains 'val2')`},
		{FilterOpLike, `("dim" like 'val1' OR "dim" like 'val2')`},
		{FilterOpNotLike, `(not "dim" like 'val1' OR not "dim" like 'val2')`},
		{FilterOpGreaterThan, `("dim" > 'val1' OR "dim" > 'val2')`},
		{FilterOpLessThan, `("dim" < 'val1' OR "dim" < 'val2')`},
		{FilterOpGreaterThanOrEqual, `("dim" >= 'val1' OR "dim" >= 'val2')`},
		{FilterOpLessThanOrEqual, `("dim" <= 'val1' OR "dim" <= 'val2')`},
	}
	for _, args := range testArgs {
		t.Run(args.operator, func(t *testing.T) {
			assert.Equal(t, args.expected, dimensionFilterExpr(DimensionFilter{
				ColumnName: "dim",
				Operator:   args.operator,
				ValueExprs: []string{`'val1'`, `'val2'`},
			}))
		})
	}
}
