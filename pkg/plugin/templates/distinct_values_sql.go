package templates

type DistinctValuesSqlParams struct {
	ColumnName           string
	TableName            string
	TimeFilterExpr       string
	DimensionFilterExprs []string
	Limit                int64
}

func RenderDistinctValuesSql(params DistinctValuesSqlParams) (string, error) {
	return RenderSingleColumnSql(SingleColumnSqlParams{
		Distinct:             true,
		ColumnName:           params.ColumnName,
		TableName:            params.TableName,
		TimeFilterExpr:       params.TimeFilterExpr,
		DimensionFilterExprs: params.DimensionFilterExprs,
		Limit:                params.Limit,
	})
}
