package dataquery

import (
	"context"
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
)

// ExpandSubqueryMacros returns a copy of `filters` with the MacroEngine applied to each filter's
// non-empty SubqueryExpr. Filters without a subquery are passed through unchanged.
//
// This is needed because builder-mode queries don't run the full SQL through MacroEngine the way
// code-mode does — they construct SQL from structured params and inline filters via
// FilterExprsFrom. A SubqueryExpr that originates from a Pinot Query variable's backing SQL may
// contain macros like $__table(), $__timeFilter("col"), $__from, $__to that Pinot can't parse,
// so we expand them here using the same engine code-mode uses.
func ExpandSubqueryMacros(ctx context.Context, filters []DimensionFilter, engine MacroEngine) ([]DimensionFilter, error) {
	out := make([]DimensionFilter, len(filters))
	for i, f := range filters {
		out[i] = f
		if f.SubqueryExpr == "" {
			continue
		}
		expanded, err := engine.ExpandMacros(ctx, f.SubqueryExpr)
		if err != nil {
			return nil, err
		}
		out[i].SubqueryExpr = expanded
	}
	return out, nil
}

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
			// Skip the row if the user is mid-edit and column/operator aren't populated yet — otherwise
			// we'd render `("" in (subquery))` and break SQL preview / resource requests.
			if filter.ColumnName == "" || filter.Operator == "" {
				continue
			}
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
