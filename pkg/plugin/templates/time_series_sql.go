package templates

import (
	"text/template"
)

var timeSeriesSqlTemplate = template.Must(template.New("pinot/time-series-sql").Parse(`
{{ .QueryOptionsExpr }}

SELECT{{ range .GroupByColumns }}
    "{{ . }}",
    {{- end }}
    {{.TimeGroupExpr}} AS "{{.TimeColumnAlias}}",
    {{.AggregationFunction}}("{{.MetricColumn}}") AS "{{.MetricColumnAlias}}"
FROM
    "{{.TableName}}"
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
    "{{.TimeColumnAlias}}" DESC
{{- end }}
LIMIT {{.Limit}};
`))

type TimeSeriesSqlParams struct {
	TableName            string
	GroupByColumns       []string
	MetricColumn         string
	AggregationFunction  string
	TimeFilterExpr       string
	TimeGroupExpr        string
	TimeColumnAlias      string
	MetricColumnAlias    string
	DimensionFilterExprs []string
	Limit                int64
	OrderByExprs         []string
	QueryOptionsExpr     string
}

func RenderTimeSeriesSql(params TimeSeriesSqlParams) (string, error) {
	return render(timeSeriesSqlTemplate, params)
}
