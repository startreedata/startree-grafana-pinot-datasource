package dataquery

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"strings"
)

func OrderByExprs(orderByClauses []OrderByClause) []string {
	orderByExprs := make([]string, 0, len(orderByClauses))
	for _, o := range orderByClauses {
		if o.ColumnName == "" {
			continue
		}
		columnExpr := pinotlib.ComplexFieldExpr(o.ColumnName, o.ColumnKey)
		orderByExprs = append(orderByExprs, pinotlib.OrderByExpr(columnExpr, o.Direction))
	}
	return orderByExprs[:]
}

func QueryOptionsExpr(options []QueryOption) string {
	exprs := make([]string, 0, len(options))
	for _, o := range options {
		if o.Name != "" && o.Value != "" {
			exprs = append(exprs, pinotlib.QueryOptionExpr(o.Name, o.Value))
		}
	}
	return strings.Join(exprs, "\n")
}

func FilterExprsFrom(filters []DimensionFilter) []string {
	exprs := make([]string, 0, len(filters))
	for _, filter := range filters {
		expr := pinotlib.ColumnFilterExpr(pinotlib.ColumnFilter{
			ColumnName: filter.ColumnName,
			ColumnKey:  filter.ColumnKey,
			ValueExprs: filter.ValueExprs,
			Operator:   pinotlib.FilterOperator(filter.Operator),
		})
		if expr == "" {
			continue
		}
		exprs = append(exprs, expr)
	}
	return exprs[:]
}
