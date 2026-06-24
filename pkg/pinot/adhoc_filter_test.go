package pinot

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdHocFilterExpr(t *testing.T) {
	tests := []struct {
		name   string
		filter AdHocFilter
		want   SqlExpr
	}{
		{name: "equals", filter: AdHocFilter{Key: "city", Operator: "=", Value: "NY"}, want: `"city" = 'NY'`},
		{name: "not equals", filter: AdHocFilter{Key: "city", Operator: "!=", Value: "NY"}, want: `"city" != 'NY'`},
		{name: "less than", filter: AdHocFilter{Key: "age", Operator: "<", Value: "30"}, want: `"age" < '30'`},
		{name: "greater than", filter: AdHocFilter{Key: "age", Operator: ">", Value: "30"}, want: `"age" > '30'`},
		{name: "regex match", filter: AdHocFilter{Key: "city", Operator: "=~", Value: "^N.*"}, want: `REGEXP_LIKE("city", '^N.*')`},
		{name: "regex not match", filter: AdHocFilter{Key: "city", Operator: "!~", Value: "^N.*"}, want: `NOT REGEXP_LIKE("city", '^N.*')`},
		{name: "escapes single quotes", filter: AdHocFilter{Key: "city", Operator: "=", Value: "O'Brien"}, want: `"city" = 'O''Brien'`},
		{name: "escapes injection attempt", filter: AdHocFilter{Key: "city", Operator: "=", Value: "x' OR '1'='1"}, want: `"city" = 'x'' OR ''1''=''1'`},
		{name: "escapes double quotes in key", filter: AdHocFilter{Key: `c" OR "1"="1`, Operator: "=", Value: "NY"}, want: `"c"" OR ""1""=""1" = 'NY'`},
		{name: "empty key", filter: AdHocFilter{Key: "", Operator: "=", Value: "NY"}, want: ""},
		{name: "empty operator", filter: AdHocFilter{Key: "city", Operator: "", Value: "NY"}, want: ""},
		{name: "unsupported operator", filter: AdHocFilter{Key: "city", Operator: "like", Value: "NY"}, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, AdHocFilterExpr(tt.filter))
		})
	}
}
