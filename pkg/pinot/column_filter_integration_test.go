package pinot

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestColumnFilterExpr_Integration verifies that the SQL fragments produced by ColumnFilterExpr
// are accepted by a real Pinot broker and return the expected rows. Unit tests in
// column_filter_test.go only check the string shape — these tests catch broker-level regressions
// (syntax that compiles in our renderer but Pinot rejects, MSE compatibility, etc.).
func TestColumnFilterExpr_Integration(t *testing.T) {
	ctx := context.Background()
	client := setupPinotAndCreateClient(t)

	allFabrics := fetchDistinctFabrics(t, ctx, client)
	require.GreaterOrEqual(t, len(allFabrics), 5, "benchmark table should have at least 5 distinct fabrics")

	t.Run("IN tuple with multiple values", func(t *testing.T) {
		expr := ColumnFilterExpr(ColumnFilter{
			ColumnName: "fabric",
			Operator:   FilterOpIn,
			ValueExprs: []string{"'fabric_0000'", "'fabric_0001'"},
		})
		assert.Equal(t, SqlExpr(`("fabric" in ('fabric_0000', 'fabric_0001'))`), expr)

		got := selectFabricsWhere(t, ctx, client, string(expr))
		assert.Equal(t, []string{"fabric_0000", "fabric_0001"}, got)
	})

	t.Run("NOT IN tuple excludes matching values", func(t *testing.T) {
		expr := ColumnFilterExpr(ColumnFilter{
			ColumnName: "fabric",
			Operator:   FilterOpNotIn,
			ValueExprs: []string{"'fabric_0000'", "'fabric_0001'"},
		})
		assert.Equal(t, SqlExpr(`("fabric" not in ('fabric_0000', 'fabric_0001'))`), expr)

		got := selectFabricsWhere(t, ctx, client, string(expr))
		// Should exclude the two filtered values and contain everything else.
		assert.NotContains(t, got, "fabric_0000")
		assert.NotContains(t, got, "fabric_0001")
		assert.Equal(t, len(allFabrics)-2, len(got))
	})

	t.Run("IN single value produces tuple syntax (regression)", func(t *testing.T) {
		// Before the fix, single-value IN generated `in 'fabric_0000'` (no parens), which Pinot rejects.
		expr := ColumnFilterExpr(ColumnFilter{
			ColumnName: "fabric",
			Operator:   FilterOpIn,
			ValueExprs: []string{"'fabric_0000'"},
		})
		assert.Equal(t, SqlExpr(`("fabric" in ('fabric_0000'))`), expr)

		got := selectFabricsWhere(t, ctx, client, string(expr))
		assert.Equal(t, []string{"fabric_0000"}, got)
	})

	t.Run("subquery in IN clause executes (MSE on)", func(t *testing.T) {
		// Mirrors the SQL FilterExprsFrom emits for subqueryExpr filters.
		sql := `SELECT DISTINCT fabric FROM benchmark
				WHERE ("fabric" in (SELECT DISTINCT fabric FROM benchmark LIMIT 1000))
				ORDER BY fabric LIMIT 1000`
		query := NewSqlQuery(sql)
		query.QueryOptions = []QueryOption{{Name: "useMultiStageEngine", Value: "true"}}

		resp, err := client.ExecuteSqlQuery(ctx, query)
		require.NoError(t, err)
		require.Empty(t, resp.Exceptions)
		assert.Equal(t, len(allFabrics), len(resp.ResultTable.Rows))
	})

	t.Run("NOT IN derived-table subquery executes (MSE on)", func(t *testing.T) {
		// Mirrors the shape buildFilterSubqueryReplacement emits for the "all-but-N selected" path.
		// The inner subquery is wrapped in a derived table so trailing LIMIT/ORDER BY can't break the WHERE.
		sql := `SELECT DISTINCT fabric FROM benchmark
				WHERE ("fabric" in (
					SELECT "fabric" FROM (SELECT DISTINCT fabric FROM benchmark LIMIT 1000)
					WHERE "fabric" NOT IN ('fabric_0000', 'fabric_0001')
				))
				ORDER BY fabric LIMIT 1000`
		query := NewSqlQuery(sql)
		query.QueryOptions = []QueryOption{{Name: "useMultiStageEngine", Value: "true"}}

		resp, err := client.ExecuteSqlQuery(ctx, query)
		require.NoError(t, err)
		require.Empty(t, resp.Exceptions)

		got := make([]string, 0, len(resp.ResultTable.Rows))
		for _, row := range resp.ResultTable.Rows {
			got = append(got, row[0].(string))
		}
		assert.NotContains(t, got, "fabric_0000")
		assert.NotContains(t, got, "fabric_0001")
		assert.Equal(t, len(allFabrics)-2, len(got))
	})
}

func fetchDistinctFabrics(t *testing.T, ctx context.Context, client *Client) []string {
	t.Helper()
	resp, err := client.ExecuteSqlQuery(ctx, NewSqlQuery(`SELECT DISTINCT fabric FROM benchmark ORDER BY fabric LIMIT 1000`))
	require.NoError(t, err)
	require.Empty(t, resp.Exceptions)

	out := make([]string, 0, len(resp.ResultTable.Rows))
	for _, row := range resp.ResultTable.Rows {
		out = append(out, row[0].(string))
	}
	return out
}

func selectFabricsWhere(t *testing.T, ctx context.Context, client *Client, whereExpr string) []string {
	t.Helper()
	sql := fmt.Sprintf(`SELECT DISTINCT fabric FROM benchmark WHERE %s ORDER BY fabric LIMIT 1000`, whereExpr)
	resp, err := client.ExecuteSqlQuery(ctx, NewSqlQuery(sql))
	require.NoError(t, err)
	require.Empty(t, resp.Exceptions)

	out := make([]string, 0, len(resp.ResultTable.Rows))
	for _, row := range resp.ResultTable.Rows {
		out = append(out, row[0].(string))
	}
	return out
}
