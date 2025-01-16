package templates

import (
	"text/template"
)

var singleMetricSqlTemplate = template.Must(template.New("pinot/single-metric-sql").Parse(`
SELECT
    {{.MetricColumnExpr}} AS {{.MetricColumnAliasExpr}},
    "{{.TimeColumn}}" AS {{.TimeColumnAliasExpr}}
FROM
    {{.TableNameExpr}}
WHERE
    {{.MetricColumnExpr}} IS NOT NULL
    AND {{.TimeFilterExpr}}{{ range .DimensionFilterExprs }}
    AND {{ . }}
{{- end }}
ORDER BY {{.TimeColumnAliasExpr}} DESC
LIMIT {{.Limit}};
`))

type SingleMetricSqlParams struct {
	TableNameExpr         string
	TimeColumn            string
	TimeColumnAliasExpr   string
	MetricColumnExpr      string
	MetricColumnAliasExpr string
	TimeFilterExpr        string
	DimensionFilterExprs  []string
	Limit                 int64
}

func RenderSingleMetricSql(params SingleMetricSqlParams) (string, error) {
	return render(singleMetricSqlTemplate, params)
}
