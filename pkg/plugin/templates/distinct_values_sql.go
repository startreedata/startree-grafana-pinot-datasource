package templates

import (
	"bytes"
	"fmt"
	"text/template"
)

const DistinctValuesSqlTemplateName = "pinot/time-series-sql"

var distinctValuesSqlTemplate = template.Must(template.New(DistinctValuesSqlTemplateName).Parse(`
SELECT DISTINCT "{{.ColumnName}}"
FROM "{{.TableName}}"
WHERE {{.TimeFilterExpr}}{{ range .DimensionFilterExprs }}
    AND {{ . }}
{{- end }}
ORDER BY "{{.ColumnName}}" ASC
`))

type DistinctValuesSqlParams struct {
	ColumnName           string
	TableName            string
	TimeFilterExpr       string
	DimensionFilterExprs []string
}

func RenderDistinctValuesSql(params DistinctValuesSqlParams) (string, error) {
	var buf bytes.Buffer
	if err := distinctValuesSqlTemplate.Execute(&buf, params); err != nil {
		return "", fmt.Errorf("failed execute template: %w", err)
	}
	return buf.String(), nil
}
