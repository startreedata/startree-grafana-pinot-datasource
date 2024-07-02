package templates

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
)

const TimeSeriesSqlTemplateName = "pinot/time-series-sql"

var timeSeriesSqlTemplate = template.Must(template.New(TimeSeriesSqlTemplateName).Parse(`
SELECT{{ range .DimensionColumns }}
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
GROUP BY{{ range .DimensionColumns }}
    "{{ . }}",
    {{- end }}
    {{.TimeGroupExpr}}
ORDER BY "{{.TimeColumnAlias}}" ASC
LIMIT 1000000
`))

type TimeSeriesSqlParams struct {
	TableName            string
	DimensionColumns     []string
	TimeColumn           string
	MetricColumn         string
	AggregationFunction  string
	TimeFilterExpr       string
	TimeGroupExpr        string
	TimeColumnAlias      string
	MetricColumnAlias    string
	DimensionFilterExprs []string
}

func IsValidAggregationFunction(aggregationFunction string) bool {
	var aggregationFunctions = []string{"SUM", "COUNT", "AVG", "MAX"}
	for i := range aggregationFunctions {
		if aggregationFunctions[i] == strings.ToUpper(aggregationFunction) {
			return true
		}
	}
	return false
}

func RenderTimeSeriesSql(params TimeSeriesSqlParams) (string, error) {
	var buf bytes.Buffer
	if err := timeSeriesSqlTemplate.Execute(&buf, params); err != nil {
		return "", fmt.Errorf("failed execute template: %w", err)
	}
	return buf.String(), nil
}
