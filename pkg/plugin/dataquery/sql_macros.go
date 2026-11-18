package dataquery

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
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
	// MacroNoTimeRange marks a query that ignores the dashboard time range on purpose.
	// The frontend recognizes it in comments or SQL code. In SQL code, it expands to nothing.
	MacroNoTimeRange = "noTimeRange"
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

	// UseExactTimeFilterBounds keeps variable filters inside the picker range when granularity is omitted.
	UseExactTimeFilterBounds bool

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
		x.ExpandNoTimeRange,
	} {
		query, err = macro(ctx, query)
		if err != nil {
			return "", err
		}
	}
	return strings.TrimSpace(query), nil
}

func (x MacroEngine) ExpandNoTimeRange(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroNoTimeRange, func(_ []string) (string, error) {
		return "", nil
	})
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
		return x.renderTimeFilter(ctx, col, argAt(args, 1)), nil
	})
}

func (x MacroEngine) renderTimeFilter(ctx context.Context, col timeColumnRef, granularityExpr string) string {
	filter := col.timeFilter(x.TimeRange)
	granularityExpr = pinot.UnquoteStringLiteral(granularityExpr)
	if x.UseExactTimeFilterBounds && granularityExpr == "" {
		// For discrete epoch units, rounding both ends up preserves the half-open range.
		bucketSize := col.format.MinimumGranularity().Duration()
		epoch := time.Unix(0, 0)
		// time.Truncate uses Go's zero time as its origin.
		offset := epoch.Sub(epoch.Truncate(bucketSize))
		filter.From = ceilTimeToBucket(filter.From, bucketSize, offset)
		filter.To = ceilTimeToBucket(filter.To, bucketSize, offset)
		return pinot.TimeFilterExpr(filter).String()
	}
	granularity := ResolveGranularity(ctx, granularityExpr, col.format, x.IntervalSize,
		pinot.DerivedGranularitiesFor(x.TableConfigs, col.name, OutputTimeFormat()))
	return pinot.TimeFilterBucketAlignedExpr(filter, granularity.Duration()).String()
}

func ceilTimeToBucket(t time.Time, size, offset time.Duration) time.Time {
	floor := t.Add(-offset).Truncate(size).Add(offset)
	if floor.Before(t) {
		return floor.Add(size)
	}
	return floor
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
//     A column the selected table's schema doesn't have (e.g. from a join, a subquery alias or a different FROM)
//     is also compared in epoch milliseconds, with a warning. Declared non-numeric columns are errors instead of
//     silently filtering on the wrong values.
//   - A qualified name (e.g. t.ts) is compared in epoch milliseconds with a warning unless the
//     full name is a literal column in the schema. Other expressions are compared in epoch milliseconds.
func (x MacroEngine) resolveTimeColumn(arg string) (timeColumnRef, error) {
	ref, err := x.timeColumnReference(arg)
	if err != nil {
		return timeColumnRef{}, err
	}

	if ref.expr != "" {
		if qualifiedColumnName.MatchString(arg) {
			x.warn(fmt.Sprintf("The source table of column `%s` could not be determined, so the time filter assumes it holds epoch milliseconds. "+
				"If it holds epoch seconds, filter with `%s >= $__fromTime AND %s < $__toTime` instead.", arg, arg, arg))
		}
		return ref, nil
	}

	if x.TableSchema.IsEmpty() {
		// No schema to check against (e.g. no table selected): keep the historical epoch-millis behavior.
		return ref, nil
	}

	name := ref.name
	dataType, found := x.TableSchema.ColumnDataType(name)
	if !found {
		// The schema is the table picked in the editor, so columns from a joined table, a subquery alias or a
		// different FROM aren't in it. Keep the historical epoch-millis behavior rather than failing the panel.
		x.warn(fmt.Sprintf("Column `%s` is not in the schema of table `%s`, so the time filter assumes it holds epoch milliseconds. "+
			"If it holds epoch seconds, filter with `%s >= $__fromTime AND %s < $__toTime` instead.", name, x.TableName, name, name))
		return ref, nil
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
		return ref, nil
	case "LONG", "INT", "FLOAT", "DOUBLE", "BIG_DECIMAL":
		x.warn(fmt.Sprintf("Column `%s` is not a date time column, so the time filter assumes it holds epoch milliseconds. "+
			"If it holds epoch seconds, filter with `%s >= $__fromTime AND %s < $__toTime` instead, "+
			"or declare it in the schema's dateTimeFieldSpecs.", name, name, name))
	default:
		return timeColumnRef{}, fmt.Errorf("column `%s` has type %s and is not a date time column; "+
			"time macros need a date time column or a numeric epoch column", name, dataType)
	}
	return ref, nil
}

const sqlIdentifier = "[\\pL_][\\pL\\pN_$]*|\"(?:[^\"]|\"\")*\"|`(?:[^`]|``)*`"

var qualifiedColumnName = regexp.MustCompile(`^(?:` + sqlIdentifier + `)(?:\s*\.\s*(?:` + sqlIdentifier + `))+$`)

func (x MacroEngine) timeColumnReference(arg string) (timeColumnRef, error) {
	ref := timeColumnRef{name: arg, format: pinot.DateTimeFormatMillisecondsEpoch()}
	if arg == "" {
		name, err := pinot.DefaultTimeColumn(x.TableSchema, x.TableConfigs)
		ref.name = name
		return ref, err
	}
	if end, closed := skipSQLQuotedSection(arg, 0); closed && end == len(arg) {
		ref.name = arg[1 : len(arg)-1]
		return ref, nil
	}
	// Pinot permits literal dots in column names. Prefer a full schema match, and never use the
	// final segment of a qualified reference to look up a column on the editor-selected table.
	if strings.ContainsAny(arg, ".( +-*/\t\r\n") {
		if _, found := x.TableSchema.ColumnDataType(arg); !found {
			ref.name = ""
			ref.expr = pinot.SqlExpr(arg)
		}
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

// warn records a message to attach to the query response as a warning notice.
func (x MacroEngine) warn(msg string) {
	if x.Warnings == nil {
		return
	}
	for _, existing := range *x.Warnings {
		if existing == msg {
			return // Several macros on the same column would otherwise repeat the message.
		}
	}
	*x.Warnings = append(*x.Warnings, msg)
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
		col, err := x.timeColumnReference(argAt(args, 0))
		if err != nil {
			return "", err
		}
		return x.renderTimeFilter(ctx, col, argAt(args, 1)), nil
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

// expandMacro replaces every `$__<macroName>` invocation in SQL code using render.
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
		start := indexSQLMacro(query, token, pos)
		if start < 0 {
			if pos == 0 {
				return query, nil
			}
			out.WriteString(query[pos:])
			return out.String(), nil
		}
		end := start + len(token)

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

// hasSQLMacro checks for active invocations of the named macros.
func hasSQLMacro(query string, names ...string) bool {
	for _, name := range names {
		if indexSQLMacro(query, "$__"+name, 0) >= 0 {
			return true
		}
	}
	return false
}

func indexSQLMacro(query, token string, start int) int {
	if !strings.Contains(query[start:], token) {
		return -1
	}
	for i := start; i < len(query); {
		if next := skipSQLNonCode(query, i); next > i {
			i = next
			continue
		}
		if strings.HasPrefix(query[i:], token) {
			end := i + len(token)
			if end == len(query) || !isIdentChar(query[end]) {
				return i
			}
		}
		i++
	}
	return -1
}

func isIdentChar(c byte) bool {
	return c == '_' || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9')
}

// skipSQLNonCode returns the end of a comment or quoted section starting at i, or i for SQL code.
func skipSQLNonCode(s string, i int) int {
	switch {
	case strings.HasPrefix(s[i:], "--"):
		if end := strings.IndexAny(s[i+2:], "\r\n"); end >= 0 {
			return i + 2 + end
		}
		return len(s)
	case strings.HasPrefix(s[i:], "/*"):
		if end := strings.Index(s[i+2:], "*/"); end >= 0 {
			return i + 4 + end
		}
		return len(s)
	default:
		end, _ := skipSQLQuotedSection(s, i)
		return end
	}
}

func skipSQLQuotedSection(s string, i int) (end int, closed bool) {
	quote := s[i]
	if quote != '\'' && quote != '"' && quote != '`' {
		return i, false
	}
	for j := i + 1; j < len(s); j++ {
		if s[j] != quote {
			continue
		}
		if j+1 < len(s) && s[j+1] == quote {
			j++ // SQL escapes a quote by doubling it.
			continue
		}
		return j + 1, true
	}
	return len(s), false
}

// findClosingParen returns the index of the `)` matching the `(` at openIdx, skipping over
// parentheses inside comments and quoted sections.
func findClosingParen(s string, openIdx int) (int, error) {
	depth := 0
	for i := openIdx; i < len(s); {
		if next := skipSQLNonCode(s, i); next > i {
			i = next
			continue
		}
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, nil
			}
		}
		i++
	}
	return -1, fmt.Errorf("missing closing parenthesis")
}

// splitTopLevelArgs splits a macro argument list on commas outside parentheses, comments and quotes.
func splitTopLevelArgs(s string) []string {
	if strings.TrimSpace(s) == "" {
		return []string{}
	}
	var args []string
	depth := 0
	last := 0
	for i := 0; i < len(s); {
		if next := skipSQLNonCode(s, i); next > i {
			i = next
			continue
		}
		switch s[i] {
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
		i++
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
