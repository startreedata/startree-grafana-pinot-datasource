package templates

import (
	"text/template"
)

const DistinctValuesLimit = 100

var distinctValuesSqlTemplate = template.Must(template.New("pinot/distinct-values-sql").Parse(`
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
	ColumnExpr           string
	TableName            string
	TimeFilterExpr       string
	DimensionFilterExprs []string
	Limit                int64
}

func RenderDistinctValuesSql(params DistinctValuesSqlParams) (string, error) {
	if params.Limit < 1 {
		params.Limit = DistinctValuesLimit
	}
	return render(distinctValuesSqlTemplate, params)
}
