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

func (x MacroEngine) ExpandMacros(query string) (string, error) {
	var err error
	for _, macro := range []func(query string) (string, error){
		// These have to come first because the regex for TimeTo/From/Filter macros also matches.
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
	} {
		query, err = macro(query)
		if err != nil {
			return "", err
		}
	}
	return strings.TrimSpace(query), nil
}

func (x MacroEngine) ExpandTableName(query string) (string, error) {
	return expandMacro(query, "table", func(_ []string) (string, error) {
		return fmt.Sprintf(`"%s"`, x.TableName), nil
	})
}

func (x MacroEngine) ExpandTimeFilter(query string) (string, error) {
	return expandMacro(query, "timeFilter", func(args []string) (string, error) {
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
	})
}

func (x MacroEngine) ExpandTimeGroup(query string) (string, error) {
	return expandMacro(query, "timeGroup", func(args []string) (string, error) {
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
	})
}

func (x MacroEngine) ExpandTimeTo(query string) (string, error) {
	return expandMacro(query, "timeTo", func(args []string) (string, error) {
		if len(args) < 1 {
			return "", fmt.Errorf("expected 1 argument, got %d", len(args))
		}
		timeColumn := args[0]

		builder, err := TimeExpressionBuilderFor(x.TableSchema, timeColumn)
		if err != nil {
			return "", err
		}
		return builder.TimeExpr(x.To), nil
	})
}

func (x MacroEngine) ExpandTimeFrom(query string) (string, error) {
	return expandMacro(query, "timeFrom", func(args []string) (string, error) {
		if len(args) < 1 {
			return "", fmt.Errorf("expected 1 argument, got %d", len(args))
		}
		timeColumn := args[0]

		builder, err := TimeExpressionBuilderFor(x.TableSchema, timeColumn)
		if err != nil {
			return "", err
		}
		return builder.TimeExpr(x.From), nil
	})
}

func (x MacroEngine) ExpandTimeAlias(query string) (string, error) {
	return expandMacro(query, "timeAlias", func(_ []string) (string, error) {
		return fmt.Sprintf(`"%s"`, x.TimeAlias), nil
	})
}

func (x MacroEngine) ExpandMetricAlias(query string) (string, error) {
	return expandMacro(query, "metricAlias", func(_ []string) (string, error) {
		return fmt.Sprintf(`"%s"`, x.MetricAlias), nil
	})
}

func (x MacroEngine) ExpandTimeFilterMillis(query string) (string, error) {
	return expandMacro(query, "timeFilterMillis", func(args []string) (string, error) {
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
	})
}

func (x MacroEngine) ExpandTimeToMillis(query string) (string, error) {
	return expandMacro(query, "timeToMillis", func(_ []string) (string, error) {
		return fmt.Sprintf("%d", x.To.UnixMilli()), nil
	})
}

func (x MacroEngine) ExpandTimeFromMillis(query string) (string, error) {
	return expandMacro(query, "timeFromMillis", func(_ []string) (string, error) {
		return fmt.Sprintf("%d", x.From.UnixMilli()), nil
	})
}

func (x MacroEngine) ExpandGranularityMillis(query string) (string, error) {
	return expandMacro(query, "granularityMillis", func(args []string) (string, error) {
		if len(args) < 1 {
			return fmt.Sprintf("%d", x.IntervalSize.Milliseconds()), nil
		}

		duration, err := ParseGranularityExpr(unquoteLiteralString(args[0]))
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", duration.Milliseconds()), nil
	})
}

func (x MacroEngine) ExpandPanelMillis(query string) (string, error) {
	return expandMacro(query, "panelMillis", func(_ []string) (string, error) {
		return fmt.Sprintf("%d", x.To.UnixMilli()-x.From.UnixMilli()), nil
	})
}

func expandMacro(query string, macroName string, render func(args []string) (string, error)) (string, error) {
	re := regexp.MustCompile(fmt.Sprintf(`\$__%s(\([^)]*\))?`, macroName))
	for _, matches := range re.FindAllStringSubmatch(query, -1) {
		invocation, args := parseArgs(matches)
		result, err := render(args)
		if err != nil {
			line, col := invocationCoords(query, invocation)
			return "", fmt.Errorf("failed to expand macro `%s` (line %d, col %d): %w", macroName, line, col, err)
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
