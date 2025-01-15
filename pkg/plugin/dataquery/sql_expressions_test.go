package dataquery

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
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
	assert.EqualValues(t, []string{
		`("AirlineID" = 19393 OR "AirlineID" = 19790)`,
		`("ArrTime" > -2147483648)`,
		`("Cancelled" = 0)`,
		`("Carrier" like 'DL')`,
		`("Carrier" in 'DL')`,
		`("Carrier" not in 'DL')`,
	}, got)
}
