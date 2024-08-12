package plugin

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

type MacroEngine struct {
	TableName   string
	TimeAlias   string
	MetricAlias string
	TableSchema
	TimeRange
	IntervalSize time.Duration
}

type Macro struct {
	name   string
	re     *regexp.Regexp
	render func(args []string) (string, error)
}

var tableNameRegex = regexp.MustCompile(`\$__table(\([^)]*\))?`)
var timeFilterRegex = regexp.MustCompile(`\$__timeFilter(\([^)]*\))?`)
var timeGroupRegex = regexp.MustCompile(`\$__timeGroup(\([^)]*\))?`)
var timeToRegex = regexp.MustCompile(`\$__timeTo(\([^)]*\))?`)
var timeFromRegex = regexp.MustCompile(`\$__timeFrom(\([^)]*\))?`)
var metricAliasRegex = regexp.MustCompile(`\$__metricAlias(\([^)]*\))?`)
var timeAliasRegex = regexp.MustCompile(`\$__timeAlias(\([^)]*\))?`)
var timeFilterMillisRegex = regexp.MustCompile(`\$__timeFilterMillis(\([^)]*\))?`)
var timeToMillisRegex = regexp.MustCompile(`\$__timeToMillis(\([^)]*\))?`)
var timeFromMillisRegex = regexp.MustCompile(`\$__timeFromMillis(\([^)]*\))?`)
var granularityMillisRegex = regexp.MustCompile(`\$__granularityMillis(\([^)]*\))?`)
var panelMillisRegex = regexp.MustCompile(`\$__panelMillis(\([^)]*\))?`)

func (x MacroEngine) ExpandMacros(query string) (string, error) {
	macros := []Macro{
		{"timeFilterMillis", timeFilterMillisRegex, x.renderTimeFilterMillis},
		{"timeFromMillis", timeFromMillisRegex, x.renderTimeFromMillis},
		{"timeToMillis", timeToMillisRegex, x.renderTimeToMillis},
		{"table", tableNameRegex, x.renderTable},
		{"timeFilter", timeFilterRegex, x.renderTimeFilter},
		{"timeGroup", timeGroupRegex, x.renderTimeGroup},
		{"timeTo", timeToRegex, x.renderTimeTo},
		{"timeFrom", timeFromRegex, x.renderTimeFrom},
		{"timeAlias", timeAliasRegex, x.renderTimeAlias},
		{"metricAlias", metricAliasRegex, x.renderMetricAlias},
		{"granularityMillis", granularityMillisRegex, x.renderGranularityMillis},
		{"panelMillis", panelMillisRegex, x.renderPanelMillis},
	}

	var err error
	for _, macro := range macros {
		query, err = expandSingleMacro(query, macro)
		if err != nil {
			return "", err
		}
	}
	return strings.TrimSpace(query), nil
}

func expandSingleMacro(query string, macro Macro) (string, error) {
	for _, matches := range macro.re.FindAllStringSubmatch(query, -1) {
		invocation, args := parseArgs(matches)
		result, err := macro.render(args)
		if err != nil {
			line, col := invocationCoords(query, invocation)
			return "", fmt.Errorf("failed to expand macro `%s` (line %d, col %d): %w", macro.name, line, col, err)
		}
		query = strings.Replace(query, invocation, " "+result+" ", 1)
	}
	return query, nil
}

func invocationCoords(query string, invocation string) (int, int) {
	index := strings.Index(query, invocation)
	line := 1
	col := 1
	for i := 0; i < index; i++ {
		if query[i] == '\n' {
			line++
			col = 1
		} else {
			col++
		}
	}
	return line, col
}

func parseArgs(matches []string) (string, []string) {
	if len(matches) < 2 {
		return matches[0], []string{}
	}
	argMatch := strings.TrimSpace(matches[1])
	if len(argMatch) == 0 {
		return matches[0], []string{}
	}

	argMatch = argMatch[1 : len(argMatch)-1] // Trim surrounding ().
	rawArgs := strings.Split(argMatch, ",")
	args := make([]string, len(rawArgs))
	for i := range rawArgs {
		args[i] = strings.TrimSpace(rawArgs[i])
	}
	return matches[0], args
}

func (x MacroEngine) renderTable(_ []string) (string, error) {
	return fmt.Sprintf(`"%s"`, x.TableName), nil
}

func (x MacroEngine) renderTimeFilter(args []string) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("expected 1 required argument, got %d", len(args))
	}
	builder, err := TimeExpressionBuilderFor(x.TableSchema, args[0])
	if err != nil {
		return "", err
	}

	var granularityExpr string
	if len(args) > 1 {
		granularityExpr = unquoteLiteralString(args[1])
	}

	granularity, err := TimeGranularityFrom(granularityExpr, x.IntervalSize)
	if err != nil {
		return "", err
	}

	return builder.TimeFilterBucketAlignedExpr(x.TimeRange, granularity.Size), nil
}

func (x MacroEngine) renderTimeGroup(args []string) (string, error) {
	if len(args) < 1 || len(args) > 2 {
		return "", fmt.Errorf("expected 1 required argument, got %d", len(args))
	}
	timeColumn := args[0]

	builder, err := TimeExpressionBuilderFor(x.TableSchema, timeColumn)
	if err != nil {
		return "", err
	}

	var granularityExpr string
	if len(args) > 1 {
		granularityExpr = unquoteLiteralString(args[1])
	}

	granularity, err := TimeGranularityFrom(granularityExpr, x.IntervalSize)
	if err != nil {
		return "", err
	}

	return builder.TimeGroupExpr(granularity.Expr), nil
}

func (x MacroEngine) renderTimeTo(args []string) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("expected 1 argument, got %d", len(args))
	}
	timeColumn := args[0]

	builder, err := TimeExpressionBuilderFor(x.TableSchema, timeColumn)
	if err != nil {
		return "", err
	}
	return builder.TimeExpr(x.To), nil
}

func (x MacroEngine) renderTimeFrom(args []string) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("expected 1 argument, got %d", len(args))
	}
	timeColumn := args[0]

	builder, err := TimeExpressionBuilderFor(x.TableSchema, timeColumn)
	if err != nil {
		return "", err
	}
	return builder.TimeExpr(x.From), nil
}

func (x MacroEngine) renderTimeAlias(_ []string) (string, error) {
	return fmt.Sprintf(`"%s"`, x.TimeAlias), nil
}

func (x MacroEngine) renderMetricAlias(_ []string) (string, error) {
	return fmt.Sprintf(`"%s"`, x.MetricAlias), nil
}

func (x MacroEngine) renderTimeFilterMillis(args []string) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("expected 1 required argument, got %d", len(args))
	}
	timeColumn := unquoteLiteralName(args[0])
	builder, err := NewTimeExpressionBuilder(timeColumn, FormatMillisecondsEpoch)
	if err != nil {
		return "", err
	}

	var granularityExpr string
	if len(args) > 1 {
		granularityExpr = unquoteLiteralString(args[1])
	}

	granularity, err := TimeGranularityFrom(granularityExpr, x.IntervalSize)
	if err != nil {
		return "", err
	}

	return builder.TimeFilterBucketAlignedExpr(x.TimeRange, granularity.Size), nil
}

func (x MacroEngine) renderTimeToMillis(_ []string) (string, error) {
	return fmt.Sprintf("%d", x.To.UnixMilli()), nil

}

func (x MacroEngine) renderTimeFromMillis(_ []string) (string, error) {
	return fmt.Sprintf("%d", x.From.UnixMilli()), nil
}

func (x MacroEngine) renderGranularityMillis(args []string) (string, error) {
	if len(args) < 1 {
		return fmt.Sprintf("%d", x.IntervalSize.Milliseconds()), nil
	}

	duration, err := ParseGranularityExpr(unquoteLiteralString(args[0]))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d", duration.Milliseconds()), nil
}

func (x MacroEngine) renderPanelMillis(_ []string) (string, error) {
	return fmt.Sprintf("%d", x.To.UnixMilli()-x.From.UnixMilli()), nil
}

func unquoteLiteralName(s string) string {
	if (strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`)) ||
		(strings.HasPrefix(s, "`") && strings.HasSuffix(s, "`")) {
		return s[1 : len(s)-1]
	} else {
		return s
	}
}

func unquoteLiteralString(s string) string {
	if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") {
		return s[1 : len(s)-1]
	} else {
		return s
	}
}
