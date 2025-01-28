package templates

import (
	"text/template"
)

var timeSeriesSqlTemplate = template.Must(template.New("pinot/time-series-sql").Parse(`
SELECT{{ range .GroupByColumnExprs }}
    {{ .Expr }}{{if .Alias}} AS '{{.Alias}}'{{end}},
    {{- end }}
    {{.TimeGroupExpr}} AS {{.TimeColumnAliasExpr}},
    {{.AggregationFunction}}({{.MetricColumnExpr}}) AS {{.MetricColumnAliasExpr}}
FROM
    {{.TableNameExpr}}
WHERE
    {{.TimeFilterExpr}}{{ range .DimensionFilterExprs }}
    AND {{ . }}
{{- end }}
GROUP BY{{ range .GroupByColumnExprs }}
    {{ .Expr }},
    {{- end }}
    {{ .TimeColumnAliasExpr }}
{{- $sep := ""}}
ORDER BY{{ range $index, $element := .OrderByExprs }}{{$sep}}
    {{ $element }}{{$sep = ","}}
{{- else }}
    {{.TimeColumnAliasExpr}} DESC
{{- end }}
LIMIT {{.Limit}};
`))

type ExprWithAlias struct {
	Expr  string
	Alias string
}

type TimeSeriesSqlParams struct {
	TableNameExpr         string
	GroupByColumnExprs    []ExprWithAlias
	MetricColumnExpr      string
	AggregationFunction   string
	TimeFilterExpr        string
	TimeGroupExpr         string
	TimeColumnAliasExpr   string
	MetricColumnAliasExpr string
	DimensionFilterExprs  []string
	Limit                 int64
	OrderByExprs          []string
}

func RenderTimeSeriesSql(params TimeSeriesSqlParams) (string, error) {
	return render(timeSeriesSqlTemplate, params)
}
