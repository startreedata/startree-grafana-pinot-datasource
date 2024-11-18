package dataquery

import (
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"regexp"
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
)

type MacroEngine struct {
	TableName   string
	TimeAlias   string
	MetricAlias string
	pinotlib.TableSchema
	TimeRange
	IntervalSize time.Duration
}

func MacroExprFor(macroName string, args ...string) string {
	return fmt.Sprintf("$__%s(%s)", macroName, strings.Join(args, ", "))
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
	return expandMacro(query, MacroTable, func(_ []string) (string, error) {
		return fmt.Sprintf(`"%s"`, x.TableName), nil
	})
}

func (x MacroEngine) ExpandTimeFilter(query string) (string, error) {
	return expandMacro(query, MacroTimeFilter, func(args []string) (string, error) {
		if len(args) < 1 {
			return "", fmt.Errorf("expected 1 required argument, got %d", len(args))
		}
		timeColumn := unquoteObjectName(args[0])
		builder := getTimeExpressionBuilderOrFallback(x.TableSchema, timeColumn)

		var granularityExpr string
		if len(args) > 1 {
			granularityExpr = unquoteStringLiteral(args[1])
		}

		granularity, err := TimeGranularityFrom(granularityExpr, x.IntervalSize)
		if err != nil {
			return "", err
		}

		return builder.TimeFilterBucketAlignedExpr(x.TimeRange.From, x.TimeRange.To, granularity.Size), nil
	})
}

func getTimeExpressionBuilderOrFallback(tableSchema pinotlib.TableSchema, timeColumn string) pinotlib.TimeExpressionBuilder {
	builder, err := pinotlib.TimeExpressionBuilderFor(tableSchema, timeColumn)
	if err != nil {
		log.WithError(err).Info("Cannot build time expressions.", "timeColumn", timeColumn)
		builder, _ = pinotlib.NewTimeExpressionBuilder(timeColumn, pinotlib.FormatMillisecondsEpoch)
	}
	return builder
}

func (x MacroEngine) ExpandTimeGroup(query string) (string, error) {
	return expandMacro(query, MacroTimeGroup, func(args []string) (string, error) {
		if len(args) < 1 || len(args) > 2 {
			return "", fmt.Errorf("expected 1 required argument, got %d", len(args))
		}
		timeColumn := unquoteObjectName(args[0])
		builder := getTimeExpressionBuilderOrFallback(x.TableSchema, timeColumn)

		var granularityExpr string
		if len(args) > 1 {
			granularityExpr = unquoteStringLiteral(args[1])
		}

		granularity, err := TimeGranularityFrom(granularityExpr, x.IntervalSize)
		if err != nil {
			return "", err
		}

		return builder.TimeGroupExpr(granularity.Expr), nil
	})
}

func (x MacroEngine) ExpandTimeTo(query string) (string, error) {
	return expandMacro(query, MacroTimeTo, func(args []string) (string, error) {
		if len(args) < 1 {
			return "", fmt.Errorf("expected 1 argument, got %d", len(args))
		}
		timeColumn := unquoteObjectName(args[0])
		builder := getTimeExpressionBuilderOrFallback(x.TableSchema, timeColumn)
		return builder.TimeExpr(x.To), nil
	})
}

func (x MacroEngine) ExpandTimeFrom(query string) (string, error) {
	return expandMacro(query, MacroTimeFrom, func(args []string) (string, error) {
		if len(args) < 1 {
			return "", fmt.Errorf("expected 1 argument, got %d", len(args))
		}
		timeColumn := unquoteObjectName(args[0])
		builder := getTimeExpressionBuilderOrFallback(x.TableSchema, timeColumn)
		return builder.TimeExpr(x.From), nil
	})
}

func (x MacroEngine) ExpandTimeAlias(query string) (string, error) {
	return expandMacro(query, MacroTimeAlias, func(_ []string) (string, error) {
		return fmt.Sprintf(`"%s"`, x.TimeAlias), nil
	})
}

func (x MacroEngine) ExpandMetricAlias(query string) (string, error) {
	return expandMacro(query, MacroMetricAlias, func(_ []string) (string, error) {
		return fmt.Sprintf(`"%s"`, x.MetricAlias), nil
	})
}

func (x MacroEngine) ExpandTimeFilterMillis(query string) (string, error) {
	return expandMacro(query, MacroTimeFilterMillis, func(args []string) (string, error) {
		if len(args) < 1 {
			return "", fmt.Errorf("expected 1 required argument, got %d", len(args))
		}
		timeColumn := unquoteObjectName(args[0])
		builder, err := pinotlib.NewTimeExpressionBuilder(timeColumn, pinotlib.FormatMillisecondsEpoch)
		if err != nil {
			return "", err
		}

		var granularityExpr string
		if len(args) > 1 {
			granularityExpr = unquoteStringLiteral(args[1])
		}

		granularity, err := TimeGranularityFrom(granularityExpr, x.IntervalSize)
		if err != nil {
			return "", err
		}

		return builder.TimeFilterBucketAlignedExpr(x.TimeRange.From, x.TimeRange.To, granularity.Size), nil
	})
}

func (x MacroEngine) ExpandTimeToMillis(query string) (string, error) {
	return expandMacro(query, MacroTimeToMillis, func(_ []string) (string, error) {
		return fmt.Sprintf("%d", x.To.UnixMilli()), nil
	})
}

func (x MacroEngine) ExpandTimeFromMillis(query string) (string, error) {
	return expandMacro(query, MacroTimeFromMillis, func(_ []string) (string, error) {
		return fmt.Sprintf("%d", x.From.UnixMilli()), nil
	})
}

func (x MacroEngine) ExpandGranularityMillis(query string) (string, error) {
	return expandMacro(query, MacroGranularityMillis, func(args []string) (string, error) {
		if len(args) < 1 {
			return fmt.Sprintf("%d", x.IntervalSize.Milliseconds()), nil
		}

		duration, err := ParseGranularityExpr(unquoteStringLiteral(args[0]))
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", duration.Milliseconds()), nil
	})
}

func (x MacroEngine) ExpandPanelMillis(query string) (string, error) {
	return expandMacro(query, MacroPanelMillis, func(_ []string) (string, error) {
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

func unquoteObjectName(s string) string {
	if (strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`)) ||
		(strings.HasPrefix(s, "`") && strings.HasSuffix(s, "`")) {
		return s[1 : len(s)-1]
	} else {
		return s
	}
}

func unquoteStringLiteral(s string) string {
	if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") {
		return s[1 : len(s)-1]
	} else {
		return s
	}
}
