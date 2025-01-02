package templates

import "text/template"

var logSqlTemplate = template.Must(template.New("pinot/log-sql").Parse(`
{{ .QueryOptionsExpr }}

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
	TableNameExpr        string
	TimeColumn           string
	LogColumnExpr        string
	LogColumnAlias       string
	MetadataColumns      []ExprWithAlias
	TimeFilterExpr       string
	DimensionFilterExprs []string
	Limit                int64
	QueryOptionsExpr     string
}

func RenderLogSql(params LogSqlParams) (string, error) {
	return render(logSqlTemplate, params)
}
