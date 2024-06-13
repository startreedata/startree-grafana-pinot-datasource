package plugin

import (
	"fmt"
	"regexp"
	"strings"
)

var macros = []sqlMacro{
	{
		name:   "__tableName",
		render: renderTableNameMacro,
	},
	{
		name:   "__timeFilter",
		render: renderTimeFilterMacro,
	},
	{
		name:   "__timeGroup",
		render: renderTimeGroupMacro,
	},
	{
		name:   "__timeColumn",
		render: nil,
	},
}

type sqlMacro struct {
	name   string
	render func(queryCtx QueryContext, args []string) (string, error)
}

func ExpandMacros(queryCtx QueryContext, input string) (string, error) {
	var err error
	for _, macro := range macros {
		input, err = expandSingleMacro(queryCtx, input, macro)
		if err != nil {
			return "", err
		}
	}
	input = strings.TrimSpace(queryCtx.SqlContext.RawSql)
	return input, nil
}

func expandSingleMacro(queryCtx QueryContext, input string, macro sqlMacro) (string, error) {
	// TODO: Compile these at startup?
	re := regexp.MustCompile(macro.name + `\s*(\([^)]*\))?`)
	for _, matches := range re.FindAllStringSubmatch(input, -1) {
		invocation, args := parseArgs(matches)
		result, err := macro.render(queryCtx, args)
		if err != nil {
			return "", fmt.Errorf("failed to expand macro `%s`: %w", macro.name, err)
		}
		input = strings.Replace(input, invocation, " "+result+" ", 1)
	}
	return input, nil
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

func renderTableNameMacro(queryCtx QueryContext, _ []string) (string, error) {
	return queryCtx.TableName, nil
}

func renderTimeFilterMacro(queryCtx QueryContext, args []string) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("expected 1 argument, got %d", len(args))
	}
	builder, err := TimeExpressionBuilderFor(queryCtx, args[0])
	if err != nil {
		return "", err
	}
	return builder.BuildTimeFilterExpr(queryCtx.TimeRange), nil
}

func renderTimeGroupMacro(queryCtx QueryContext, args []string) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("expected 1 argument, got %d", len(args))
	}
	builder, err := TimeExpressionBuilderFor(queryCtx, args[0])
	if err != nil {
		return "", err
	}
	return builder.BuildTimeGroupExpr(queryCtx.IntervalSize), nil
}
