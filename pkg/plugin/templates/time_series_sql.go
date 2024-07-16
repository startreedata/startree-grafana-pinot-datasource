package templates

import (
	"bytes"
	"fmt"
	"text/template"
)

const TimeSeriesSqlTemplateName = "pinot/time-series-sql"

var timeSeriesSqlTemplate = template.Must(template.New(TimeSeriesSqlTemplateName).Parse(`
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
ORDER BY "{{.TimeColumnAlias}}" DESC
LIMIT {{.Limit}}
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
}

func RenderTimeSeriesSql(params TimeSeriesSqlParams) (string, error) {
	var buf bytes.Buffer
	if err := timeSeriesSqlTemplate.Execute(&buf, params); err != nil {
		return "", fmt.Errorf("failed execute template: %w", err)
	}
	return buf.String(), nil
}
