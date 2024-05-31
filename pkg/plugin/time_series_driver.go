package plugin

import "text/template"

type TimeSeriesContext struct {
	TableName           string
	TimeColumn          string
	MetricColumn        string
	DimensionColumns    []string
	AggregationFunction string
	TableSchema         TableSchema
}

var timeSeriesSqlTemplate = template.Must(template.New("pinot-time-series-template").Parse(`
SELECT
    {{ range .DimensionColumns }} "{{ . }}", {{ end }}
	"{{.TimeColumn}}" AS ts, 
	"{{.MetricColumn}}" AS met
FROM
    "{{.TableName}}"
WHERE
    {{.TimeFilterExp}}
`))

var timeSeriesAggSqlTemplate = template.Must(template.New("pinot-time-series-agg-template").Parse(`
SELECT
	{{ range .DimensionColumns }} {{ . }}, {{ end }}
	{{.TimeGroupExpr}} AS ts, 
	{{.AggregationFunction}}("{{.MetricColumn}}") AS met
FROM
    "{{.TableName}}"
WHERE
    {{.TimeFilterExp}}
GROUP BY
	{{ range .DimensionColumns }} {{ . }}, {{ end }}
    {{.TimeGroupExp}}
`))

type templArgs struct {
	TableName           string
	DimensionColumns    []string
	TimeColumn          string
	MetricColumn        string
	AggregationFunction string
	TimeFilterExp       string
	TimeGroupExp        string
}

type TimeSeriesDriver struct{}

func (p TimeSeriesDriver) RenderPinotSql(queryCtx QueryContext) (string, error) {
	exprBuilder, err := TimeExpressionBuilderFor(queryCtx, queryCtx.TimeSeriesContext.TimeColumn)
	if err != nil {
		return "", err
	}

	_ = templArgs{
		TableName:           queryCtx.TableName,
		TimeColumn:          queryCtx.TimeSeriesContext.TimeColumn,
		MetricColumn:        queryCtx.TimeSeriesContext.MetricColumn,
		DimensionColumns:    queryCtx.TimeSeriesContext.DimensionColumns,
		TimeFilterExp:       exprBuilder.BuildTimeFilterExpr(queryCtx.TimeRange),
		TimeGroupExp:        exprBuilder.BuildTimeGroupExpr(queryCtx.IntervalSize),
		AggregationFunction: queryCtx.TimeSeriesContext.AggregationFunction,
	}

	newQueryCtx, err := ExpandMacros(queryCtx)
	if err != nil {
		return "", err
	}
	return newQueryCtx.SqlContext.RawSql, nil
}
