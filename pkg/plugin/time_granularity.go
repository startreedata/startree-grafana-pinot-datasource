package plugin

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const GranularityAuto = "auto"

// TimeGranularity stores the Pinot expression and golang duration for a given granularity.
type TimeGranularity struct {
	Expr string
	Size time.Duration
}

// TimeGranularityFrom returns a new Time Granularity based on the given Pinot expression and default size.
// If granularityExpr is empty or "auto", then the default size is used.
// Otherwise, the granularity expression is used.
func TimeGranularityFrom(granularityExpr string, defaultSize time.Duration) (TimeGranularity, error) {
	if granularityExpr == "" || granularityExpr == GranularityAuto {
		return TimeGranularity{
			Expr: GranularityExprFrom(defaultSize),
			Size: defaultSize,
		}, nil
	}

	size, err := ParseGranularityExpr(granularityExpr)
	if err != nil {
		return TimeGranularity{}, err
	}

	return TimeGranularity{
		Expr: granularityExpr,
		Size: size,
	}, nil
}

func GranularityExprFrom(bucketSize time.Duration) string {
	switch {
	case bucketSize.Hours() >= 1:
		return fmt.Sprintf("%d:HOURS", int(bucketSize.Hours()))
	case bucketSize.Minutes() >= 1:
		return fmt.Sprintf("%d:MINUTES", int(bucketSize.Minutes()))
	case bucketSize.Seconds() >= 1:
		return fmt.Sprintf("%d:SECONDS", int(bucketSize.Seconds()))
	case bucketSize.Milliseconds() >= 1:
		return fmt.Sprintf("%d:MILLISECONDS", int(bucketSize.Milliseconds()))
	case bucketSize.Microseconds() >= 1:
		return fmt.Sprintf("%d:MICROSECONDS", int(bucketSize.Microseconds()))
	default:
		return fmt.Sprintf("%d:NANOSECONDS", int(bucketSize.Nanoseconds()))
	}
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
