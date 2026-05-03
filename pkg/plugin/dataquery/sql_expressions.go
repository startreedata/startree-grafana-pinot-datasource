package dataquery

import (
	"fmt"

	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
)

func OrderByExprs(orderByClauses []OrderByClause) []pinot.SqlExpr {
	orderByExprs := make([]pinot.SqlExpr, 0, len(orderByClauses))
	for _, o := range orderByClauses {
		if o.ColumnName == "" {
			continue
		}
		columnExpr := pinot.ComplexFieldExpr(o.ColumnName, o.ColumnKey)
		orderByExprs = append(orderByExprs, pinot.OrderByExpr(columnExpr, o.Direction))
	}
	return orderByExprs[:]
}

func FilterExprsFrom(filters []DimensionFilter) []pinot.SqlExpr {
	exprs := make([]pinot.SqlExpr, 0, len(filters))
	for _, filter := range filters {
		if filter.SubqueryExpr != "" {
			// Subquery path: frontend sends a pre-built subquery instead of literal values.
			// e.g. { columnName: "entity", operator: "in", subqueryExpr: "SELECT DISTINCT entity FROM t" }
			// → ("entity" in (SELECT DISTINCT entity FROM t))
			// Negating operators (!=, not in) produce NOT IN instead.
			columnExpr := pinot.ComplexFieldExpr(filter.ColumnName, filter.ColumnKey)
			op := pinot.GetInOrNotInFromOperator(pinot.FilterOperator(filter.Operator))
			exprs = append(exprs, pinot.SqlExpr(fmt.Sprintf(`(%s %s (%s))`, columnExpr, op, filter.SubqueryExpr)))
			continue
		}

		expr := pinot.ColumnFilterExpr(pinot.ColumnFilter{
			ColumnName: filter.ColumnName,
			ColumnKey:  filter.ColumnKey,
			ValueExprs: filter.ValueExprs,
			Operator:   pinot.FilterOperator(filter.Operator),
		})
		if expr == "" {
			continue
		}
		exprs = append(exprs, expr)
	}
	return exprs[:]
}
