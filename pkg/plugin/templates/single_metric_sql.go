package templates

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
)

const SingleMetricSqlTemplateName = "pinot/single-metric-sql"

var singleMetricSqlTemplate = template.Must(template.New(SingleMetricSqlTemplateName).Parse(`
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
LIMIT {{.Limit}}
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
}

func RenderSingleMetricSql(params SingleMetricSqlParams) (string, error) {
	var buf bytes.Buffer
	if err := singleMetricSqlTemplate.Execute(&buf, params); err != nil {
		return "", fmt.Errorf("failed execute template: %w", err)
	}
	return strings.TrimSpace(buf.String()), nil
}
