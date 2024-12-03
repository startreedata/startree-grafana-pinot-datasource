package dataquery

import (
	"context"
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"sort"
	"strconv"
	"strings"
	"time"
)

const GranularityAuto = "auto"

func ResolveGranularity(ctx context.Context, expr string, fallback time.Duration, derived []pinotlib.Granularity) pinotlib.Granularity {
	if expr == "" || expr == GranularityAuto {
		return resolveAutoGranularity(fallback, derived)
	}

	granularity, err := pinotlib.ParseGranularityExpr(expr)
	if err != nil {
		log.WithError(err).FromContext(ctx).Info("Failed to parse user provided granularity; using fallback")
		return resolveAutoGranularity(fallback, derived)
	}
	return granularity
}

func resolveAutoGranularity(fallback time.Duration, derived []pinotlib.Granularity) pinotlib.Granularity {
	sort.Slice(derived, func(i, j int) bool { return derived[i].Duration() < derived[j].Duration() })

	for _, option := range derived {
		if option.Duration() > fallback {
			return option
		}
	}
	return pinotlib.GranularityOf(fallback)
}

func ParseGranularityExpr(granularity string) (time.Duration, error) {
	var timeSize int64
	var timeUnit string
	fields := strings.SplitN(granularity, ":", 2)
	if len(fields) == 1 {
		timeSize = 1
		timeUnit = fields[0]
	} else {
		timeSize, _ = strconv.ParseInt(fields[0], 10, 64)
		if timeSize < 1 {
			timeSize = 1
		}
		timeUnit = fields[1]
	}

	switch strings.ToUpper(timeUnit) {
	case "NANOSECONDS":
		return time.Duration(timeSize) * time.Nanosecond, nil
	case "MICROSECONDS":
		return time.Duration(timeSize) * time.Microsecond, nil
	case "MILLISECONDS":
		return time.Duration(timeSize) * time.Millisecond, nil
	case "SECONDS":
		return time.Duration(timeSize) * time.Second, nil
	case "MINUTES":
		return time.Duration(timeSize) * time.Minute, nil
	case "HOURS":
		return time.Duration(timeSize) * time.Hour, nil
	case "DAYS":
		return time.Duration(timeSize) * time.Hour * 24, nil
	default:
		return 0, fmt.Errorf("unknown time unit `%s`", timeUnit)
	}
}
