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

func (x MacroEngine) ExpandMacros(query string) (string, error) {
	macros := []Macro{
		{"table", tableNameRegex, x.renderTable},
		{"timeFilter", timeFilterRegex, x.renderTimeFilter},
		{"timeGroup", timeGroupRegex, x.renderTimeGroup},
		{"timeTo", timeToRegex, x.renderTimeTo},
		{"timeFrom", timeFromRegex, x.renderTimeFrom},
		{"timeAlias", timeAliasRegex, x.renderTimeAlias},
		{"metricAlias", metricAliasRegex, x.renderMetricAlias},
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
	if len(args) != 1 {
		return "", fmt.Errorf("expected 1 argument, got %d", len(args))
	}
	builder, err := TimeExpressionBuilderFor(x.TableSchema, args[0])
	if err != nil {
		return "", err
	}
	return builder.TimeFilterExpr(x.TimeRange), nil
}

func (x MacroEngine) renderTimeGroup(args []string) (string, error) {
	if len(args) < 1 || len(args) > 2 {
		return "", fmt.Errorf("expected 1-2 arguments, got %d", len(args))
	}
	timeColumn := args[0]

	builder, err := TimeExpressionBuilderFor(x.TableSchema, timeColumn)
	if err != nil {
		return "", err
	}

	granularity := builder.GranularityExpr(x.IntervalSize)
	if len(args) == 2 {
		granularity = args[1]
	}

	return builder.BuildTimeGroupExpr(granularity), nil
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
	builder, err := TimeExpressionBuilderFor(x.TableSchema, args[0])
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
