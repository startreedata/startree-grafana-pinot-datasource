package templates

import (
	"text/template"
)

const DistinctValuesLimit = 100

var distinctValuesSqlTemplate = template.Must(template.New("pinot/distinct-values-sql").Parse(`
SELECT DISTINCT {{.ColumnExpr}}
FROM "{{.TableName}}"
{{- if .TimeFilterExpr }}
{{- /* At this time, there is no need for dimension filters without a time filter. */}}
WHERE {{.TimeFilterExpr}}{{ range .DimensionFilterExprs }}
    AND {{ . }}
{{- end }}
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
