package pinotlib

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
)

const DistinctValuesLimit = 100

var distinctValuesSqlTemplate = template.Must(template.New("distinct-values-sql").Parse(`
SELECT DISTINCT {{.ColumnExpr}}
FROM "{{.TableName}}"
WHERE {{.ColumnExpr}} IS NOT NULL {{- if .TimeFilterExpr }}
    AND {{.TimeFilterExpr}}{{end}}
    {{- range .DimensionFilterExprs }}
    AND {{ . }}
{{- end }}
ORDER BY {{.ColumnExpr}} ASC
LIMIT {{.Limit}};
`))

type DistinctValuesSqlParams struct {
	ColumnExpr           SqlExpr
	TableName            string
	TimeFilterExpr       SqlExpr
	DimensionFilterExprs []SqlExpr
	Limit                int64
}

func RenderDistinctValuesSql(params DistinctValuesSqlParams) (string, error) {
	if params.Limit < 1 {
		params.Limit = DistinctValuesLimit
	}
	return render(distinctValuesSqlTemplate, params)
}

var timeSeriesSqlTemplate = template.Must(template.New("time-series-sql").Parse(`
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
	Expr  SqlExpr
	Alias string
}

type TimeSeriesSqlParams struct {
	TableNameExpr         SqlExpr
	GroupByColumnExprs    []ExprWithAlias
	MetricColumnExpr      SqlExpr
	AggregationFunction   string
	TimeFilterExpr        SqlExpr
	TimeGroupExpr         SqlExpr
	TimeColumnAliasExpr   SqlExpr
	MetricColumnAliasExpr SqlExpr
	DimensionFilterExprs  []SqlExpr
	Limit                 int64
	OrderByExprs          []SqlExpr
}

func RenderTimeSeriesSql(params TimeSeriesSqlParams) (string, error) {
	return render(timeSeriesSqlTemplate, params)
}

var singleMetricSqlTemplate = template.Must(template.New("single-metric-sql").Parse(`
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
	TableNameExpr         SqlExpr
	TimeColumn            string
	TimeColumnAliasExpr   SqlExpr
	MetricColumnExpr      SqlExpr
	MetricColumnAliasExpr SqlExpr
	TimeFilterExpr        SqlExpr
	DimensionFilterExprs  []SqlExpr
	Limit                 int64
}

func RenderSingleMetricSql(params SingleMetricSqlParams) (string, error) {
	return render(singleMetricSqlTemplate, params)
}

var logSqlTemplate = template.Must(template.New("log-sql").Parse(`
SELECT
    {{ .LogColumnExpr }}{{ if .LogColumnAlias }} AS '{{ .LogColumnAlias }}'{{ end }},
    {{- range .MetadataColumns}}
    {{ .Expr }}{{ if .Alias }} AS '{{ .Alias }}'{{ end }},
    {{- end }}
    "{{ .TimeColumn }}"
FROM {{ .TableNameExpr }}
WHERE {{ .LogColumnExpr }} IS NOT NULL
    {{- if .TimeFilterExpr }}
    AND {{ .TimeFilterExpr }}
    {{- end }}
    {{- range .DimensionFilterExprs }}
    AND {{ . }}
    {{- end }}
ORDER BY
    "{{ .TimeColumn }}" ASC,
    {{ if .LogColumnAlias }}"{{ .LogColumnAlias }}"{{ else }}{{ .LogColumnExpr }}{{ end }} ASC
LIMIT {{ .Limit }};
`))

type LogSqlParams struct {
	TableNameExpr        SqlExpr
	TimeColumn           string
	LogColumnExpr        SqlExpr
	LogColumnAlias       string
	MetadataColumns      []ExprWithAlias
	TimeFilterExpr       SqlExpr
	DimensionFilterExprs []SqlExpr
	Limit                int64
}

func RenderLogSql(params LogSqlParams) (string, error) {
	return render(logSqlTemplate, params)
}

func render(tmpl *template.Template, params interface{}) (string, error) {
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, params); err != nil {
		return "", fmt.Errorf("failed execute template: %w", err)
	}
	return strings.TrimSpace(buf.String()), nil
}
