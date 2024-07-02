package plugin

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
)

func DimensionFilterExpr(filter DimensionFilter) string {
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
		default:
			return "1=1"
		}
	}

	exprs := make([]string, len(filter.ValueExprs))
	for i, expr := range filter.ValueExprs {
		exprs[i] = format(expr)
	}
	return fmt.Sprintf(`(%s)`, strings.Join(exprs, " AND "))
}
