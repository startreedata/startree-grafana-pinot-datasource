package dataquery

import (
	"context"
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
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
	TableName    string
	TimeAlias    string
	MetricAlias  string
	TableSchema  pinot.TableSchema
	TableConfigs pinot.ListTableConfigsResponse
	TimeRange
	IntervalSize time.Duration
}

func MacroExprFor(macroName string, args ...string) pinot.SqlExpr {
	return pinot.SqlExpr(fmt.Sprintf("$__%s(%s)", macroName, strings.Join(args, ", ")))
}

func (x MacroEngine) ExpandMacros(ctx context.Context, query string) (string, error) {
	var err error
	for _, macro := range []func(ctx context.Context, query string) (string, error){
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
		if len(args) < 1 {
			return "", fmt.Errorf("expected 1 required argument, got %d", len(args))
		}
		timeColumn := pinot.UnquoteObjectName(args[0])

		var granularityExpr string
		if len(args) > 1 {
			granularityExpr = pinot.UnquoteStringLiteral(args[1])
		}

		format := getDateTimeFormatOrFallback(x.TableSchema, timeColumn)
		derived := pinot.DerivedGranularitiesFor(x.TableConfigs, timeColumn, OutputTimeFormat())
		granularity := ResolveGranularity(ctx, granularityExpr, format, x.IntervalSize, derived)
		return pinot.TimeFilterBucketAlignedExpr(pinot.TimeFilter{
			Column: timeColumn,
			Format: format,
			From:   x.TimeRange.From,
			To:     x.TimeRange.To,
		}, granularity.Duration()).String(), nil
	})
}

func getDateTimeFormatOrFallback(tableSchema pinot.TableSchema, timeColumn string) pinot.DateTimeFormat {
	format, err := pinot.GetTimeColumnFormat(tableSchema, timeColumn)
	if err != nil {
		return pinot.DateTimeFormatMillisecondsEpoch()
	}
	return format
}

func (x MacroEngine) ExpandTimeGroup(ctx context.Context, query string) (string, error) {
	return expandMacro(query, MacroTimeGroup, func(args []string) (string, error) {
		if len(args) < 1 || len(args) > 2 {
			// TODO: Fix confusing error since 2 args is also valid.
			return "", fmt.Errorf("expected 1 required argument, got %d", len(args))
		}
		timeColumn := pinot.UnquoteObjectName(args[0])

		var granularityExpr string
		if len(args) > 1 {
			granularityExpr = pinot.UnquoteStringLiteral(args[1])
		}

		format := getDateTimeFormatOrFallback(x.TableSchema, timeColumn)
		derived := pinot.DerivedGranularitiesFor(x.TableConfigs, timeColumn, OutputTimeFormat())
		granularity := ResolveGranularity(ctx, granularityExpr, format, x.IntervalSize, derived)
		return pinot.TimeGroupExpr(x.TableConfigs, pinot.DateTimeConversion{
			TimeColumn:   timeColumn,
			InputFormat:  format,
			OutputFormat: pinot.DateTimeFormatMillisecondsEpoch(),
			Granularity:  granularity,
		}).String(), nil
	})
}

func (x MacroEngine) ExpandTimeTo(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroTimeTo, func(args []string) (string, error) {
		if len(args) < 1 {
			return "", fmt.Errorf("expected 1 argument, got %d", len(args))
		}
		timeColumn := pinot.UnquoteObjectName(args[0])
		format := getDateTimeFormatOrFallback(x.TableSchema, timeColumn)
		return pinot.TimeExpr(x.To, format).String(), nil
	})
}

func (x MacroEngine) ExpandTimeFrom(_ context.Context, query string) (string, error) {
	return expandMacro(query, MacroTimeFrom, func(args []string) (string, error) {
		if len(args) < 1 {
			return "", fmt.Errorf("expected 1 argument, got %d", len(args))
		}
		timeColumn := pinot.UnquoteObjectName(args[0])
		format := getDateTimeFormatOrFallback(x.TableSchema, timeColumn)
		return pinot.TimeExpr(x.From, format).String(), nil
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
		if len(args) < 1 {
			return "", fmt.Errorf("expected 1 required argument, got %d", len(args))
		}
		timeColumn := pinot.UnquoteObjectName(args[0])

		var granularityExpr string
		if len(args) > 1 {
			granularityExpr = pinot.UnquoteStringLiteral(args[1])
		}

		format := pinot.DateTimeFormatMillisecondsEpoch()
		derived := pinot.DerivedGranularitiesFor(x.TableConfigs, timeColumn, OutputTimeFormat())
		granularity := ResolveGranularity(ctx, granularityExpr, format, x.IntervalSize, derived)
		return pinot.TimeFilterBucketAlignedExpr(pinot.TimeFilter{
			Column: timeColumn,
			Format: format,
			From:   x.TimeRange.From,
			To:     x.TimeRange.To,
		}, granularity.Duration()).String(), nil
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
