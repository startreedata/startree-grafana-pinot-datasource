package templates

import "text/template"

const SingleColumnLimit = 100

var singleColumnSqlTemplate = template.Must(template.New("pinot/single-column-sql").Parse(`
SELECT{{if .Distinct}} DISTINCT{{end}} "{{.ColumnName}}"
FROM "{{.TableName}}"
{{- if .TimeFilterExpr }}
{{- /* At this time, there is no need for dimension filters without a time filter. */}}
WHERE {{.TimeFilterExpr}}{{ range .DimensionFilterExprs }}
    AND {{ . }}
{{- end }}
{{- end }}
ORDER BY "{{.ColumnName}}" ASC
LIMIT {{.Limit}};
`))

type SingleColumnSqlParams struct {
	ColumnName           string
	Distinct             bool
	TableName            string
	TimeFilterExpr       string
	DimensionFilterExprs []string
	Limit                int64
}

func RenderSingleColumnSql(params SingleColumnSqlParams) (string, error) {
	if params.Limit < 1 {
		params.Limit = SingleColumnLimit
	}
	return render(singleColumnSqlTemplate, params)
}
