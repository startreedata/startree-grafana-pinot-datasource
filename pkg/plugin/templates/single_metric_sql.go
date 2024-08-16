package templates

import (
	"text/template"
)

var singleMetricSqlTemplate = template.Must(template.New("pinot/single-metric-sql").Parse(`
{{.QueryOptionsExpr}}

SELECT
    "{{.MetricColumn}}" AS "{{.MetricColumnAlias}}",
    "{{.TimeColumn}}" AS "{{.TimeColumnAlias}}"
FROM
    "{{.TableName}}"
WHERE
    "{{.MetricColumn}}" IS NOT NULL
    AND {{.TimeFilterExpr}}{{ range .DimensionFilterExprs }}
    AND {{ . }}
{{- end }}
ORDER BY "{{.TimeColumnAlias}}" DESC
LIMIT {{.Limit}};
`))

type SingleMetricSqlParams struct {
	TableName            string
	TimeColumn           string
	TimeColumnAlias      string
	MetricColumn         string
	MetricColumnAlias    string
	TimeFilterExpr       string
	DimensionFilterExprs []string
	Limit                int64
	QueryOptionsExpr     string
}

func RenderSingleMetricSql(params SingleMetricSqlParams) (string, error) {
	return render(singleMetricSqlTemplate, params)
}
