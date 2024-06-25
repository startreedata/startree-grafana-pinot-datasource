package plugin

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

type MacroEngine struct {
	TableName string
	TableSchema
	TimeRange
	IntervalSize time.Duration
}

type Macro struct {
	name   string
	re     *regexp.Regexp
	render func(args []string) (string, error)
}

var tableNameRegex = regexp.MustCompile(`__tableName\s*(\([^)]*\))?`)
var timeFilterRegex = regexp.MustCompile(`__timeFilter\s*(\([^)]*\))?`)
var timeGroupRegex = regexp.MustCompile(`__timeGroup\s*(\([^)]*\))?`)

func (x MacroEngine) ExpandMacros(query string) (string, error) {
	macros := []Macro{
		{"__tableName", tableNameRegex, x.renderTableNameMacro},
		{"__timeFilter", timeFilterRegex, x.renderTimeFilterMacro},
		{"__timeGroup", timeGroupRegex, x.renderTimeGroupMacro},
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
	// TODO: Compile these at startup?
	for _, matches := range macro.re.FindAllStringSubmatch(query, -1) {
		invocation, args := parseArgs(matches)
		result, err := macro.render(args)
		if err != nil {
			return "", fmt.Errorf("failed to expand macro `%s`: %w", macro.name, err)
		}
		query = strings.Replace(query, invocation, " "+result+" ", 1)
	}
	return query, nil
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

func (x MacroEngine) renderTableNameMacro(_ []string) (string, error) {
	return x.TableName, nil
}

func (x MacroEngine) renderTimeFilterMacro(args []string) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("expected 1 argument, got %d", len(args))
	}
	builder, err := TimeExpressionBuilderFor(x.TableSchema, args[0])
	if err != nil {
		return "", err
	}
	return builder.BuildTimeFilterExpr(x.TimeRange), nil
}

func (x MacroEngine) renderTimeGroupMacro(args []string) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("expected 1 argument, got %d", len(args))
	}
	builder, err := TimeExpressionBuilderFor(x.TableSchema, args[0])
	if err != nil {
		return "", err
	}
	return builder.BuildTimeGroupExpr(x.IntervalSize), nil
}
