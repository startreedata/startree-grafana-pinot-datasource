package templates

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
)

func render(tmpl *template.Template, params interface{}) (string, error) {
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, params); err != nil {
		return "", fmt.Errorf("failed execute template: %w", err)
	}
	return strings.TrimSpace(buf.String()), nil
}
