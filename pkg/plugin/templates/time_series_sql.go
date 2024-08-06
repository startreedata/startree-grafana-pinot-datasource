package templates

import (
	"bytes"
	"fmt"
	"strings"
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
{{- $sep := ""}}
ORDER BY{{ range $index, $element := .OrderByExprs }}{{$sep}}
    {{ $element }}{{$sep = ","}}
{{- else }}
    "{{.TimeColumnAlias}}" DESC
{{- end }}
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
	OrderByExprs         []string
}

func RenderTimeSeriesSql(params TimeSeriesSqlParams) (string, error) {
	var buf bytes.Buffer
	if err := timeSeriesSqlTemplate.Execute(&buf, params); err != nil {
		return "", fmt.Errorf("failed execute template: %w", err)
	}
	return strings.TrimSpace(buf.String()), nil
}
