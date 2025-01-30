package dataquery

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
)

func OrderByExprs(orderByClauses []OrderByClause) []pinotlib.SqlExpr {
	orderByExprs := make([]pinotlib.SqlExpr, 0, len(orderByClauses))
	for _, o := range orderByClauses {
		if o.ColumnName == "" {
			continue
		}
		columnExpr := pinotlib.ComplexFieldExpr(o.ColumnName, o.ColumnKey)
		orderByExprs = append(orderByExprs, pinotlib.OrderByExpr(columnExpr, o.Direction))
	}
	return orderByExprs[:]
}

func FilterExprsFrom(filters []DimensionFilter) []pinotlib.SqlExpr {
	exprs := make([]pinotlib.SqlExpr, 0, len(filters))
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
