package dataquery

import (
	"context"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
)

var _ ExecutableQuery = TableBuilderQuery{}

// TableBuilderQuery renders a general-purpose table/aggregation query: a projection of
// dimensions (GROUP BY columns) and one or more aggregations, filtered by the dashboard time
// range and any dimension filters, with optional ORDER BY and LIMIT.
type TableBuilderQuery struct {
	TimeRange        TimeRange
	TableName        string
	TimeColumn       string
	Dimensions       []ComplexField
	Aggregations     []Aggregation
	DimensionFilters []DimensionFilter
	OrderByClauses   []OrderByClause
	QueryOptions     []QueryOption
	Limit            int64
}

func (query TableBuilderQuery) Validate() error {
	switch {
	case query.TableName == "":
		return errors.New("TableName is required")
	case query.TimeColumn == "":
		return errors.New("TimeColumn is required")
	case len(query.Dimensions) == 0 && len(query.validAggregations()) == 0:
		return errors.New("at least one dimension or aggregation is required")
	default:
		return nil
	}
}

func (query TableBuilderQuery) Execute(client *pinot.Client, ctx context.Context) backend.DataResponse {
	if err := query.Validate(); err != nil {
		return NewBadRequestErrorResponse(err)
	}

	sqlQuery, err := query.RenderSqlQuery(ctx, client)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, client, sqlQuery)
	if !ok {
		return backendResp
	}

	frame, err := ExtractTableDataFrame(results, query.TimeColumn)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	return NewSqlQueryDataResponse(frame, exceptions)
}

func (query TableBuilderQuery) RenderSqlQuery(ctx context.Context, client *pinot.Client) (pinot.SqlQuery, error) {
	schema, err := client.GetTableSchema(ctx, query.TableName)
	if err != nil {
		return pinot.SqlQuery{}, err
	}

	timeColumnFormat, err := pinot.GetTimeColumnFormat(schema, query.TimeColumn)
	if err != nil {
		return pinot.SqlQuery{}, err
	}

	// Expand Grafana plugin macros inside any filter's SubqueryExpr (e.g. a Pinot Query variable's
	// backing SQL). Same rationale as the time-series and logs builders.
	expandedFilters, err := ExpandSubqueryMacros(ctx, query.DimensionFilters, MacroEngine{
		TableName:   query.TableName,
		TableSchema: schema,
		TimeRange:   query.TimeRange,
		TimeAlias:   BuilderTimeColumn,
	})
	if err != nil {
		return pinot.SqlQuery{}, err
	}

	sql, err := pinot.RenderTableSql(pinot.TableSqlParams{
		TableNameExpr:        pinot.ObjectExpr(query.TableName),
		SelectExprs:          query.selectExprs(),
		GroupByExprs:         query.groupByExprs(),
		DimensionFilterExprs: FilterExprsFrom(expandedFilters),
		OrderByExprs:         OrderByExprs(query.OrderByClauses),
		Limit:                query.resolveLimit(),
		TimeFilterExpr: pinot.TimeFilterExpr(pinot.TimeFilter{
			Column: query.TimeColumn,
			Format: timeColumnFormat,
			From:   query.TimeRange.From,
			To:     query.TimeRange.To,
		}),
	})
	if err != nil {
		return pinot.SqlQuery{}, err
	}

	return newSqlQueryWithOptions(sql, query.QueryOptions), nil
}

func (query TableBuilderQuery) RenderSqlWithMacros() (string, error) {
	sql, err := pinot.RenderTableSql(pinot.TableSqlParams{
		TableNameExpr:        MacroExprFor(MacroTable),
		SelectExprs:          query.selectExprs(),
		GroupByExprs:         query.groupByExprs(),
		DimensionFilterExprs: FilterExprsFrom(query.DimensionFilters),
		OrderByExprs:         OrderByExprs(query.OrderByClauses),
		Limit:                query.resolveLimit(),
		TimeFilterExpr:       MacroExprFor(MacroTimeFilter, pinot.ObjectExpr(query.TimeColumn).String()),
	})
	if err != nil {
		return "", err
	}
	return newSqlQueryWithOptions(sql, query.QueryOptions).RenderSql(), nil
}

// selectExprs returns the projection: dimensions first, then aggregations, each fully rendered
// with its alias. Aggregations get a double-quoted identifier alias so ORDER BY can reference them.
func (query TableBuilderQuery) selectExprs() []pinot.SqlExpr {
	exprs := make([]pinot.SqlExpr, 0, len(query.Dimensions)+len(query.Aggregations))
	for _, dim := range query.Dimensions {
		if dim.Name == "" {
			continue
		}
		expr := pinot.ComplexFieldExpr(dim.Name, dim.Key)
		if alias := complexFieldAlias(dim.Name, dim.Key); alias != "" {
			expr = pinot.SqlExpr(fmt.Sprintf("%s AS '%s'", expr, alias))
		}
		exprs = append(exprs, expr)
	}
	for _, agg := range query.validAggregations() {
		exprs = append(exprs, pinot.SqlExpr(fmt.Sprintf("%s AS %s", aggregationExpr(agg), pinot.ObjectExpr(aggregationAlias(agg)))))
	}
	return exprs
}

// groupByExprs returns the dimension expressions to GROUP BY, but only when the query also
// aggregates. A pure projection (dimensions, no aggregations) needs no GROUP BY.
func (query TableBuilderQuery) groupByExprs() []pinot.SqlExpr {
	if len(query.validAggregations()) == 0 {
		return nil
	}
	exprs := make([]pinot.SqlExpr, 0, len(query.Dimensions))
	for _, dim := range query.Dimensions {
		if dim.Name == "" {
			continue
		}
		exprs = append(exprs, pinot.ComplexFieldExpr(dim.Name, dim.Key))
	}
	return exprs
}

func (query TableBuilderQuery) validAggregations() []Aggregation {
	aggs := make([]Aggregation, 0, len(query.Aggregations))
	for _, agg := range query.Aggregations {
		if agg.Function == "" {
			continue
		}
		if agg.Column.Name == "" && agg.Function != AggregationFunctionCount {
			continue
		}
		aggs = append(aggs, agg)
	}
	return aggs
}

func (query TableBuilderQuery) resolveLimit() int64 {
	if query.Limit >= 1 {
		return query.Limit
	}
	return DefaultQueryLimit
}

func aggregationExpr(agg Aggregation) pinot.SqlExpr {
	if agg.Function == AggregationFunctionCount && isCountStar(agg.Column) {
		return pinot.SqlExpr(fmt.Sprintf("%s(*)", agg.Function))
	}
	return pinot.SqlExpr(fmt.Sprintf("%s(%s)", agg.Function, pinot.ComplexFieldExpr(agg.Column.Name, agg.Column.Key)))
}

// aggregationAlias is the result column name. It must match the frontend's aggregationLabelOf so
// ORDER BY options the user picks line up with the rendered SELECT alias.
func aggregationAlias(agg Aggregation) string {
	arg := complexFieldLabel(agg.Column.Name, agg.Column.Key)
	if agg.Function == AggregationFunctionCount && isCountStar(agg.Column) {
		arg = "*"
	}
	return fmt.Sprintf("%s(%s)", agg.Function, arg)
}

// isCountStar reports whether a COUNT aggregation has no real column — either omitted or the
// literal "*" the UI shows. Both render as COUNT(*), never COUNT("*") (a column literally named *).
func isCountStar(column ComplexField) bool {
	return column.Name == "" || column.Name == "*"
}

func complexFieldLabel(name string, key string) string {
	if key == "" {
		return name
	}
	return fmt.Sprintf("%s['%s']", name, key)
}
