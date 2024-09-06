package templates

import (
	"text/template"
)

var singleMetricSqlTemplate = template.Must(template.New("pinot/single-metric-sql").Parse(`
{{.QueryOptionsExpr}}

SELECT
    "{{.MetricColumn}}" AS {{.MetricColumnAliasExpr}},
    "{{.TimeColumn}}" AS {{.TimeColumnAliasExpr}}
FROM
    {{.TableNameExpr}}
WHERE
    "{{.MetricColumn}}" IS NOT NULL
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
	MetricColumn          string
	MetricColumnAliasExpr string
	TimeFilterExpr        string
	DimensionFilterExprs  []string
	Limit                 int64
	QueryOptionsExpr      string
}

func RenderSingleMetricSql(params SingleMetricSqlParams) (string, error) {
	return render(singleMetricSqlTemplate, params)
}
