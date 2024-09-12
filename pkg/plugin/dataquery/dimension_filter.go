package dataquery

import (
	"fmt"
	"strings"
)

const (
	FilterOpEquals             = "="
	FilterOpNotEquals          = "!="
	FilterOpContains           = "contains"
	FilterOpNotContains        = "not contains"
	FilterOpLike               = "like"
	FilterOpNotLike            = "not like"
	FilterOpGreaterThan        = ">"
	FilterOpLessThan           = "<"
	FilterOpGreaterThanOrEqual = ">="
	FilterOpLessThanOrEqual    = "<="
	FilterOpIn                 = "in"
	FilterOpNotIn              = "not in"
)

func FilterExprsFrom(filters []DimensionFilter) []string {
	exprs := make([]string, 0, len(filters))
	for _, filter := range filters {
		if filter.ColumnName == "" || filter.Operator == "" || len(filter.ValueExprs) == 0 {
			continue
		}
		expr := dimensionFilterExpr(filter)
		if expr == "" {
			continue
		}
		exprs = append(exprs, expr)
	}
	return exprs[:]
}

func dimensionFilterExpr(filter DimensionFilter) string {
	format := func(valueExpr string) string {
		switch filter.Operator {
		case FilterOpEquals:
			return fmt.Sprintf(`"%s" = %s`, filter.ColumnName, valueExpr)
		case FilterOpNotEquals:
			return fmt.Sprintf(`"%s" != %s`, filter.ColumnName, valueExpr)
		case FilterOpContains:
			return fmt.Sprintf(`"%s" contains %s`, filter.ColumnName, valueExpr)
		case FilterOpNotContains:
			return fmt.Sprintf(`not "%s" contains %s`, filter.ColumnName, valueExpr)
		case FilterOpLike:
			return fmt.Sprintf(`"%s" like %s`, filter.ColumnName, valueExpr)
		case FilterOpNotLike:
			return fmt.Sprintf(`not "%s" like %s`, filter.ColumnName, valueExpr)
		case FilterOpGreaterThan:
			return fmt.Sprintf(`"%s" > %s`, filter.ColumnName, valueExpr)
		case FilterOpLessThan:
			return fmt.Sprintf(`"%s" < %s`, filter.ColumnName, valueExpr)
		case FilterOpGreaterThanOrEqual:
			return fmt.Sprintf(`"%s" >= %s`, filter.ColumnName, valueExpr)
		case FilterOpLessThanOrEqual:
			return fmt.Sprintf(`"%s" <= %s`, filter.ColumnName, valueExpr)
		case FilterOpIn:
			return fmt.Sprintf(`"%s" in %s`, filter.ColumnName, valueExpr)
		case FilterOpNotIn:
			return fmt.Sprintf(`"%s" not in %s`, filter.ColumnName, valueExpr)
		default:
			return "1=1"
		}
	}

	exprs := make([]string, len(filter.ValueExprs))
	for i, expr := range filter.ValueExprs {
		exprs[i] = format(expr)
	}
	return fmt.Sprintf(`(%s)`, strings.Join(exprs, " OR "))
}
