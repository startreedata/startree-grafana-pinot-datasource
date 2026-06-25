package pinot

import (
	"fmt"
	"strings"
)

// AdHocFilter is a single Grafana ad-hoc filter: a column, an operator, and a value. The json tags
// match the wire shape the frontend sends (Grafana's AdHocVariableFilter).
type AdHocFilter struct {
	Key      string `json:"key"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

// AdHocFilterExpr renders a single Grafana ad-hoc filter as a boolean SQL expression. Both the
// column identifier and the value are escaped to prevent SQL injection, since the key and value are
// request-controlled. It returns "" for an incomplete filter or an unsupported operator so the
// caller can skip it.
//
// Grafana sends values as strings, so they're always rendered as quoted string literals. Numeric
// `<` / `>` comparisons therefore rely on Pinot's implicit cast of the literal to the column type.
// ponytail: string-literal values only; add per-column type-aware unquoting if numeric range
// filters on big columns prove too slow.
func AdHocFilterExpr(filter AdHocFilter) SqlExpr {
	if filter.Key == "" || filter.Operator == "" {
		return ""
	}

	columnExpr := ObjectExpr(escapeIdentifier(filter.Key))
	valueExpr := StringLiteralExpr(EscapeStringLiteral(filter.Value))

	switch filter.Operator {
	case "=":
		return SqlExpr(fmt.Sprintf(`%s = %s`, columnExpr, valueExpr))
	case "!=":
		return SqlExpr(fmt.Sprintf(`%s != %s`, columnExpr, valueExpr))
	case "<":
		return SqlExpr(fmt.Sprintf(`%s < %s`, columnExpr, valueExpr))
	case ">":
		return SqlExpr(fmt.Sprintf(`%s > %s`, columnExpr, valueExpr))
	case "=~":
		return SqlExpr(fmt.Sprintf(`REGEXP_LIKE(%s, %s)`, columnExpr, valueExpr))
	case "!~":
		return SqlExpr(fmt.Sprintf(`NOT REGEXP_LIKE(%s, %s)`, columnExpr, valueExpr))
	default:
		return ""
	}
}

// escapeIdentifier escapes a name for use inside a double-quoted SQL identifier by doubling embedded
// double quotes. ObjectExpr only wraps in quotes, so escaping happens here.
func escapeIdentifier(name string) string {
	return strings.ReplaceAll(name, `"`, `""`)
}
