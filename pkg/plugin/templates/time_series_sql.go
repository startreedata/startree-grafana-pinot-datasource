package templates

import (
	"text/template"
)

var timeSeriesSqlTemplate = template.Must(template.New("pinot/time-series-sql").Parse(`
{{ .QueryOptionsExpr }}

SELECT{{ range .GroupByColumns }}
    "{{ . }}",
    {{- end }}
    {{.TimeGroupExpr}} AS {{.TimeColumnAliasExpr}},
    {{.AggregationFunction}}("{{.MetricColumn}}") AS {{.MetricColumnAliasExpr}}
FROM
    {{.TableNameExpr}}
WHERE
    {{.TimeFilterExpr}}{{ range .DimensionFilterExprs }}
    AND {{ . }}
{{- end }}
GROUP BY{{ range .GroupByColumns }}
    "{{ . }}",
    {{- end }}
    {{.TimeGroupExpr}}
{{- $sep := ""}}
ORDER BY{{ range $index, $element := .OrderByExprs }}{{$sep}}
    {{ $element }}{{$sep = ","}}
{{- else }}
    {{.TimeColumnAliasExpr}} DESC
{{- end }}
LIMIT {{.Limit}};
`))

type TimeSeriesSqlParams struct {
	TableNameExpr         string
	GroupByColumns        []string
	MetricColumn          string
	AggregationFunction   string
	TimeFilterExpr        string
	TimeGroupExpr         string
	TimeColumnAliasExpr   string
	MetricColumnAliasExpr string
	DimensionFilterExprs  []string
	Limit                 int64
	OrderByExprs          []string
	QueryOptionsExpr      string
}

func RenderTimeSeriesSql(params TimeSeriesSqlParams) (string, error) {
	return render(timeSeriesSqlTemplate, params)
}
