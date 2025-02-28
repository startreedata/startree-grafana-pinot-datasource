package dataquery

import "github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"

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
