package pinotlib

import (
	"fmt"
	"strings"
)

type ColumnFilter struct {
	ColumnName string
	ColumnKey  string
	ValueExprs []string
	Operator   FilterOperator
}

type FilterOperator string

const (
	FilterOpEquals             FilterOperator = "="
	FilterOpNotEquals          FilterOperator = "!="
	FilterOpContains           FilterOperator = "contains"
	FilterOpNotContains        FilterOperator = "not contains"
	FilterOpLike               FilterOperator = "like"
	FilterOpNotLike            FilterOperator = "not like"
	FilterOpGreaterThan        FilterOperator = ">"
	FilterOpLessThan           FilterOperator = "<"
	FilterOpGreaterThanOrEqual FilterOperator = ">="
	FilterOpLessThanOrEqual    FilterOperator = "<="
	FilterOpIn                 FilterOperator = "in"
	FilterOpNotIn              FilterOperator = "not in"
)

func ColumnFilterExpr(filter ColumnFilter) string {
	if filter.ColumnName == "" || filter.Operator == "" || len(filter.ValueExprs) == 0 {
		return ""
	}

	columnExpr := ComplexFieldExpr(filter.ColumnName, filter.ColumnKey)
	format := func(valueExpr string) string {
		switch filter.Operator {
		case FilterOpEquals:
			return fmt.Sprintf(`%s = %s`, columnExpr, valueExpr)
		case FilterOpNotEquals:
			return fmt.Sprintf(`%s != %s`, columnExpr, valueExpr)
		case FilterOpContains:
			return fmt.Sprintf(`%s contains %s`, columnExpr, valueExpr)
		case FilterOpNotContains:
			return fmt.Sprintf(`not %s contains %s`, columnExpr, valueExpr)
		case FilterOpLike:
			return fmt.Sprintf(`%s like %s`, columnExpr, valueExpr)
		case FilterOpNotLike:
			return fmt.Sprintf(`not %s like %s`, columnExpr, valueExpr)
		case FilterOpGreaterThan:
			return fmt.Sprintf(`%s > %s`, columnExpr, valueExpr)
		case FilterOpLessThan:
			return fmt.Sprintf(`%s < %s`, columnExpr, valueExpr)
		case FilterOpGreaterThanOrEqual:
			return fmt.Sprintf(`%s >= %s`, columnExpr, valueExpr)
		case FilterOpLessThanOrEqual:
			return fmt.Sprintf(`%s <= %s`, columnExpr, valueExpr)
		case FilterOpIn:
			return fmt.Sprintf(`%s in %s`, columnExpr, valueExpr)
		case FilterOpNotIn:
			return fmt.Sprintf(`%s not in %s`, columnExpr, valueExpr)
		default:
			return ""
		}
	}

	exprs := make([]string, 0, len(filter.ValueExprs))
	for _, expr := range filter.ValueExprs {
		filterExpr := format(expr)
		if filterExpr == "" {
			continue
		}
		exprs = append(exprs, format(expr))
	}
	if len(exprs) == 0 {
		return ""
	}

	return fmt.Sprintf(`(%s)`, strings.Join(exprs, " OR "))
}
