package dataquery

import (
	"context"
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"strings"
	"time"
)

const (
	MacroTable             = "table"
	MacroTimeFilter        = "timeFilter"
	MacroTimeGroup         = "timeGroup"
	MacroTimeTo            = "timeTo"
	MacroTimeFrom          = "timeFrom"
	MacroTimeAlias         = "timeAlias"
	MacroMetricAlias       = "metricAlias"
	MacroTimeFilterMillis  = "timeFilterMillis"
	MacroPanelMillis       = "panelMillis"
	MacroGranularityMillis = "granularityMillis"
	MacroTimeFromMillis    = "timeFromMillis"
	MacroTimeToMillis      = "timeToMillis"
	MacroAdHocFilter       = "adHocFilter"
	MacroFromTime          = "fromTime"
	MacroToTime            = "toTime"
	MacroIntervalSeconds   = "interval_s"
)

type MacroEngine struct {
	TableName    string
	TimeAlias    string
	MetricAlias  string
	TableSchema  pinot.TableSchema
	TableConfigs pinot.ListTableConfigsResponse
	AdHocFilters []pinot.AdHocFilter
	TimeRange
	IntervalSize time.Duration

	// Warnings, when non-nil, collects non-fatal problems found while expanding macros
	// (e.g. a guessed time column format) so they can be shown on the panel.
	Warnings *[]string
}

func MacroExprFor(macroName string, args ...string) pinot.SqlExpr {
	return pinot.SqlExpr(fmt.Sprintf("$__%s(%s)", macroName, strings.Join(args, ", ")))
}

func (x MacroEngine) ExpandMacros(ctx context.Context, query string) (string, error) {
	var err error
	for _, macro := range []func(ctx context.Context, query string) (string, error){
		x.ExpandTimeFilterMillis,
		x.ExpandTimeFromMillis,
		x.ExpandTimeToMillis,

		x.ExpandTableName,
		x.ExpandTimeFilter,
		x.ExpandTimeGroup,
		x.ExpandTimeTo,
		x.ExpandTimeAlias,
		x.ExpandMetricAlias,
		x.ExpandTimeFrom,
		x.ExpandGranularityMillis,
		x.ExpandPanelMillis,
		x.ExpandIntervalSeconds,
		x.ExpandFromTime,
		x.ExpandToTime,
		x.ExpandAdHocFilter,
	} {
		query, err = macro(ctx, query)
		if err != nil {
			return "", err
		}
	}
	return strings.TrimSpace(query), nil
}

func (x MacroEngine) ExpandTableName(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroTable, func(_ []string) (string, error) {
		return fmt.Sprintf(`"%s"`, x.TableName), nil
	})
}

func (x MacroEngine) ExpandTimeFilter(ctx context.Context, query string) (string, error) {
	return expandMacro(query, MacroTimeFilter, func(args []string) (string, error) {
		if len(args) > 2 {
			return "", fmt.Errorf("expected at most 2 arguments, got %d", len(args))
		}
		col, err := x.resolveTimeColumn(argAt(args, 0))
		if err != nil {
			return "", err
		}
		granularity := ResolveGranularity(ctx, pinot.UnquoteStringLiteral(argAt(args, 1)), col.format, x.IntervalSize,
			pinot.DerivedGranularitiesFor(x.TableConfigs, col.name, OutputTimeFormat()))
		return pinot.TimeFilterBucketAlignedExpr(col.timeFilter(x.TimeRange), granularity.Duration()).String(), nil
	})
}

// timeColumnRef is a time macro's column argument resolved against the table schema.
type timeColumnRef struct {
	name   string        // Column name (for schema and derived-granularity lookups); empty for expressions.
	expr   pinot.SqlExpr // Raw SQL to use instead of the quoted name (expressions and qualified names).
	format pinot.DateTimeFormat
}

func (c timeColumnRef) timeFilter(timeRange TimeRange) pinot.TimeFilter {
	return pinot.TimeFilter{Column: c.name, ColumnExpr: c.expr, Format: c.format, From: timeRange.From, To: timeRange.To}
}

func argAt(args []string, i int) string {
	if i < len(args) {
		return strings.TrimSpace(args[i])
	}
	return ""
}

// resolveTimeColumn turns a time macro's column argument into a column reference and its time format.
//
//   - No argument: the table's primary time column (segmentsConfig.timeColumnName, or the only date time column).
//   - A column name ("ts", `ts`, 'ts' or ts): the format comes from the schema's dateTimeFieldSpecs (or legacy
//     timeFieldSpec). A numeric column that is not declared as a date time column is assumed to hold epoch
//     milliseconds, and a warning is attached to the response because the guess may be wrong (e.g. epoch seconds).
//     Unknown and non-numeric columns are errors instead of silently filtering on the wrong values.
//   - Anything else (e.g. DATETIMECONVERT(...)) is used verbatim and compared in epoch milliseconds.
func (x MacroEngine) resolveTimeColumn(arg string) (timeColumnRef, error) {
	if arg == "" {
		name, err := pinot.DefaultTimeColumn(x.TableSchema, x.TableConfigs)
		if err != nil {
			return timeColumnRef{}, err
		}
		arg = name
	}

	if isTimeColumnExpression(arg) {
		return timeColumnRef{expr: pinot.SqlExpr(arg), format: pinot.DateTimeFormatMillisecondsEpoch()}, nil
	}

	name := unquoteColumnArg(arg)
	ref := timeColumnRef{name: name}
	if !isQuoted(arg) && strings.Contains(name, ".") {
		// Qualified name like t.ts: keep it as written and look the column up by its last segment.
		ref.expr = pinot.SqlExpr(arg)
		name = name[strings.LastIndex(name, ".")+1:]
	}

	if x.TableSchema.IsEmpty() {
		// No schema to check against (e.g. no table selected): keep the historical epoch-millis behavior.
		ref.format = pinot.DateTimeFormatMillisecondsEpoch()
		return ref, nil
	}

	dataType, found := x.TableSchema.ColumnDataType(name)
	if !found {
		return timeColumnRef{}, fmt.Errorf("column `%s` not found in table `%s`", name, x.TableName)
	}

	format, err := pinot.GetTimeColumnFormat(x.TableSchema, name)
	if err == nil {
		ref.format = format
		return ref, nil
	}
	if isDateTimeColumn(x.TableSchema, name) {
		return timeColumnRef{}, fmt.Errorf("column `%s`: %w", name, err) // Declared but unsupported format.
	}

	switch strings.ToUpper(dataType) {
	case "TIMESTAMP":
		ref.format = pinot.DateTimeFormatMillisecondsEpoch()
	case "LONG", "INT", "FLOAT", "DOUBLE", "BIG_DECIMAL":
		ref.format = pinot.DateTimeFormatMillisecondsEpoch()
		x.warn(fmt.Sprintf("Column `%s` is not a date time column, so the time filter assumes it holds epoch milliseconds. "+
			"If it holds epoch seconds, filter with `%s >= $__fromTime AND %s < $__toTime` instead, "+
			"or declare it in the schema's dateTimeFieldSpecs.", name, name, name))
	default:
		return timeColumnRef{}, fmt.Errorf("column `%s` has type %s and is not a date time column; "+
			"time macros need a date time column or a numeric epoch column", name, dataType)
	}
	return ref, nil
}

func isDateTimeColumn(schema pinot.TableSchema, name string) bool {
	for _, spec := range schema.DateTimeFieldSpecs {
		if spec.Name == name {
			return true
		}
	}
	return schema.TimeFieldSpec.Name() == name
}

func isQuoted(s string) bool {
	if len(s) < 2 {
		return false
	}
	first, last := s[0], s[len(s)-1]
	return first == last && (first == '"' || first == '`' || first == '\'')
}

func unquoteColumnArg(s string) string {
	if isQuoted(s) {
		return s[1 : len(s)-1]
	}
	return s
}

// isTimeColumnExpression reports whether a macro's column argument is an SQL expression rather than a column name.
func isTimeColumnExpression(arg string) bool {
	return !isQuoted(arg) && strings.ContainsAny(arg, "( +-*/")
}

// warn records a message to attach to the query response as a warning notice.
func (x MacroEngine) warn(msg string) {
	if x.Warnings != nil {
		*x.Warnings = append(*x.Warnings, msg)
	}
}

func (x MacroEngine) ExpandTimeGroup(ctx context.Context, query string) (string, error) {
	return expandMacro(query, MacroTimeGroup, func(args []string) (string, error) {
		if len(args) > 2 {
			return "", fmt.Errorf("expected at most 2 arguments, got %d", len(args))
		}
		col, err := x.resolveTimeColumn(argAt(args, 0))
		if err != nil {
			return "", err
		}
		if col.expr != "" {
			return "", fmt.Errorf("expected a column name, got expression `%s`", col.expr)
		}
		granularity := ResolveGranularity(ctx, pinot.UnquoteStringLiteral(argAt(args, 1)), col.format, x.IntervalSize,
			pinot.DerivedGranularitiesFor(x.TableConfigs, col.name, OutputTimeFormat()))
		return pinot.TimeGroupExpr(x.TableConfigs, pinot.DateTimeConversion{
			TimeColumn:   col.name,
			InputFormat:  col.format,
			OutputFormat: pinot.DateTimeFormatMillisecondsEpoch(),
			Granularity:  granularity,
		}).String(), nil
	})
}

func (x MacroEngine) ExpandTimeTo(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroTimeTo, func(args []string) (string, error) {
		if len(args) > 1 {
			return "", fmt.Errorf("expected at most 1 argument, got %d", len(args))
		}
		col, err := x.resolveTimeColumn(argAt(args, 0))
		if err != nil {
			return "", err
		}
		return pinot.TimeExpr(x.To, col.format).String(), nil
	})
}

func (x MacroEngine) ExpandTimeFrom(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroTimeFrom, func(args []string) (string, error) {
		if len(args) > 1 {
			return "", fmt.Errorf("expected at most 1 argument, got %d", len(args))
		}
		col, err := x.resolveTimeColumn(argAt(args, 0))
		if err != nil {
			return "", err
		}
		return pinot.TimeExpr(x.From, col.format).String(), nil
	})
}

func (x MacroEngine) ExpandTimeAlias(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroTimeAlias, func(_ []string) (string, error) {
		return fmt.Sprintf(`"%s"`, x.TimeAlias), nil
	})
}

func (x MacroEngine) ExpandMetricAlias(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroMetricAlias, func(_ []string) (string, error) {
		return fmt.Sprintf(`"%s"`, x.MetricAlias), nil
	})
}

func (x MacroEngine) ExpandTimeFilterMillis(ctx context.Context, query string) (string, error) {
	return expandMacro(query, MacroTimeFilterMillis, func(args []string) (string, error) {
		if len(args) > 2 {
			return "", fmt.Errorf("expected at most 2 arguments, got %d", len(args))
		}
		// The column is compared in epoch millis by definition, so only its name/expression is resolved here.
		arg := argAt(args, 0)
		if arg == "" {
			name, err := pinot.DefaultTimeColumn(x.TableSchema, x.TableConfigs)
			if err != nil {
				return "", err
			}
			arg = name
		}
		col := timeColumnRef{name: unquoteColumnArg(arg), format: pinot.DateTimeFormatMillisecondsEpoch()}
		if isTimeColumnExpression(arg) || (!isQuoted(arg) && strings.Contains(arg, ".")) {
			col = timeColumnRef{expr: pinot.SqlExpr(arg), format: col.format}
		}
		granularity := ResolveGranularity(ctx, pinot.UnquoteStringLiteral(argAt(args, 1)), col.format, x.IntervalSize,
			pinot.DerivedGranularitiesFor(x.TableConfigs, col.name, OutputTimeFormat()))
		return pinot.TimeFilterBucketAlignedExpr(col.timeFilter(x.TimeRange), granularity.Duration()).String(), nil
	})
}

func (x MacroEngine) ExpandTimeToMillis(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroTimeToMillis, func(_ []string) (string, error) {
		return fmt.Sprintf("%d", x.To.UnixMilli()), nil
	})
}

func (x MacroEngine) ExpandTimeFromMillis(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroTimeFromMillis, func(_ []string) (string, error) {
		return fmt.Sprintf("%d", x.From.UnixMilli()), nil
	})
}

func (x MacroEngine) ExpandGranularityMillis(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroGranularityMillis, func(args []string) (string, error) {
		if len(args) < 1 {
			return fmt.Sprintf("%d", x.IntervalSize.Milliseconds()), nil
		}

		duration, err := ParseGranularityExpr(pinot.UnquoteStringLiteral(args[0]))
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", duration.Milliseconds()), nil
	})
}

func (x MacroEngine) ExpandPanelMillis(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroPanelMillis, func(_ []string) (string, error) {
		return fmt.Sprintf("%d", x.To.UnixMilli()-x.From.UnixMilli()), nil
	})
}

// ExpandIntervalSeconds replaces $__interval_s with the time-bucket granularity in whole seconds —
// the seconds counterpart of $__granularityMillis. Sub-second intervals truncate to 0.
func (x MacroEngine) ExpandIntervalSeconds(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroIntervalSeconds, func(_ []string) (string, error) {
		return fmt.Sprintf("%d", int64(x.IntervalSize.Seconds())), nil
	})
}

// ExpandFromTime replaces $__fromTime with the start of the selected time range in epoch seconds —
// the seconds counterpart of $__timeFromMillis.
func (x MacroEngine) ExpandFromTime(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroFromTime, func(_ []string) (string, error) {
		return fmt.Sprintf("%d", x.From.Unix()), nil
	})
}

// ExpandToTime replaces $__toTime with the end of the selected time range in epoch seconds —
// the seconds counterpart of $__timeToMillis.
func (x MacroEngine) ExpandToTime(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroToTime, func(_ []string) (string, error) {
		return fmt.Sprintf("%d", x.To.Unix()), nil
	})
}

// ExpandAdHocFilter replaces $__adHocFilter with the AND-joined boolean expression of the query's
// Grafana ad-hoc filters. With no filters it expands to TRUE so `WHERE $__adHocFilter` is a safe
// no-op. Identifiers are quoted and values escaped by pinot.AdHocFilterExpr.
func (x MacroEngine) ExpandAdHocFilter(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroAdHocFilter, func(_ []string) (string, error) {
		return adHocFilterWhereExpr(x.AdHocFilters).String(), nil
	})
}

func adHocFilterWhereExpr(filters []pinot.AdHocFilter) pinot.SqlExpr {
	exprs := make([]string, 0, len(filters))
	for _, filter := range filters {
		expr := pinot.AdHocFilterExpr(filter)
		if expr == "" {
			continue
		}
		exprs = append(exprs, expr.String())
	}
	if len(exprs) == 0 {
		return "TRUE"
	}
	return pinot.SqlExpr(strings.Join(exprs, " AND "))
}

// expandMacro replaces every `$__<macroName>` invocation in query using render.
//
// An invocation is the macro name followed by an optional parenthesised argument list. The name must
// end at a non-identifier character, so `$__timeFilter` never matches inside `$__timeFilterMillis`.
// Arguments may contain nested parentheses and quoted strings, e.g.
// `$__timeFilter(DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES'))`;
// they are split only on top-level commas.
func expandMacro(query string, macroName string, render func(args []string) (string, error)) (string, error) {
	token := "$__" + macroName
	var out strings.Builder
	pos := 0
	for {
		idx := strings.Index(query[pos:], token)
		if idx < 0 {
			out.WriteString(query[pos:])
			return out.String(), nil
		}
		start := pos + idx
		end := start + len(token)
		if end < len(query) && isIdentChar(query[end]) {
			// A longer macro name (e.g. $__timeFilterMillis); not ours.
			out.WriteString(query[pos:end])
			pos = end
			continue
		}

		args := []string{}
		if end < len(query) && query[end] == '(' {
			closeIdx, err := findClosingParen(query, end)
			if err != nil {
				line, col := offsetCoords(query, start)
				return "", fmt.Errorf("failed to expand macro `%s` (line %d, col %d): %w", macroName, line, col, err)
			}
			args = splitTopLevelArgs(query[end+1 : closeIdx])
			end = closeIdx + 1
		}

		result, err := render(args)
		if err != nil {
			line, col := offsetCoords(query, start)
			return "", fmt.Errorf("failed to expand macro `%s` (line %d, col %d): %w", macroName, line, col, err)
		}
		out.WriteString(query[pos:start])
		out.WriteString(" " + result + " ")
		pos = end
	}
}

func isIdentChar(c byte) bool {
	return c == '_' || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9')
}

// findClosingParen returns the index of the `)` matching the `(` at openIdx, skipping over
// parentheses inside '...', "..." and `...` quoted sections.
func findClosingParen(s string, openIdx int) (int, error) {
	depth := 0
	var quote byte
	for i := openIdx; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}
	return -1, fmt.Errorf("missing closing parenthesis")
}

// splitTopLevelArgs splits a macro argument list on commas that are not nested in parentheses or quotes.
func splitTopLevelArgs(s string) []string {
	if strings.TrimSpace(s) == "" {
		return []string{}
	}
	var args []string
	depth := 0
	var quote byte
	last := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(s[last:i]))
				last = i + 1
			}
		}
	}
	return append(args, strings.TrimSpace(s[last:]))
}

func offsetCoords(query string, index int) (int, int) {
	line := 1
	col := 1
	for i := 0; i < index && i < len(query); i++ {
		if query[i] == '\n' {
			line++
			col = 1
		} else {
			col++
		}
	}
	return line, col
}
