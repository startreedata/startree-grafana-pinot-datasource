package pinotlib

import (
	"fmt"
	"strings"
	"time"
)

func ObjectExpr(obj string) string {
	return fmt.Sprintf(`"%s"`, obj)
}

func StringLiteralExpr(lit string) string {
	return fmt.Sprintf(`'%s'`, lit)
}

func UnquoteObjectName(s string) string {
	if (strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`)) ||
		(strings.HasPrefix(s, "`") && strings.HasSuffix(s, "`")) {
		return s[1 : len(s)-1]
	} else {
		return s
	}
}

func UnquoteStringLiteral(s string) string {
	if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") {
		return s[1 : len(s)-1]
	} else {
		return s
	}
}

type TimeFilter struct {
	Column string
	Format DateTimeFormat
	From   time.Time
	To     time.Time
}

func TimeFilterBucketAlignedExpr(filter TimeFilter, bucketSize time.Duration) string {
	fromTrunc := filter.From.Truncate(bucketSize)
	toTrunc := filter.To.Truncate(bucketSize)
	if toTrunc.Before(filter.To) {
		toTrunc = toTrunc.Add(bucketSize)
	}

	return TimeFilterExpr(TimeFilter{
		Column: filter.Column,
		Format: filter.Format,
		From:   fromTrunc,
		To:     toTrunc,
	})
}

func TimeFilterExpr(filter TimeFilter) string {
	return fmt.Sprintf(`%s >= %s AND %s < %s`,
		ObjectExpr(filter.Column), TimeExpr(filter.From, filter.Format),
		ObjectExpr(filter.Column), TimeExpr(filter.To, filter.Format),
	)
}

func TimeExpr(ts time.Time, format DateTimeFormat) string {
	switch format.Unit {
	case TimeUnitNanoseconds:
		return fmt.Sprintf("%d", ts.UnixNano()/int64(format.Size))
	case TimeUnitMicroseconds:
		return fmt.Sprintf("%d", ts.UnixMicro()/int64(format.Size))
	case TimeUnitMilliseconds:
		return fmt.Sprintf("%d", ts.UnixMilli()/int64(format.Size))
	case TimeUnitSeconds:
		return fmt.Sprintf("%d", ts.Unix()/int64(format.Size))
	case TimeUnitMinutes:
		return fmt.Sprintf("%d", ts.Unix()/int64(format.Size)/60)
	case TimeUnitHours:
		return fmt.Sprintf("%d", ts.Unix()/int64(format.Size)/3600)
	case TimeUnitDays:
		return fmt.Sprintf("%d", ts.Unix()/int64(format.Size)/86400)
	default:
		return ""
	}
}

func GranularityExpr(granularity Granularity) string {
	return StringLiteralExpr(granularity.String())
}

func DateTimeFormatExpr(format DateTimeFormat) string {
	return StringLiteralExpr(format.LegacyString())
}

func TimeGroupExpr(configs ListTableConfigsResponse, timeGroup DateTimeConversion) string {
	derivedColumns := DerivedTimeColumnsFrom(configs)
	for _, col := range derivedColumns {
		if col.Source.Equals(timeGroup) {
			return ObjectExpr(col.ColumnName)
		}
	}
	return fmt.Sprintf(`DATETIMECONVERT(%s, %s, %s, %s)`,
		ObjectExpr(timeGroup.TimeColumn),
		DateTimeFormatExpr(timeGroup.InputFormat),
		DateTimeFormatExpr(timeGroup.OutputFormat),
		GranularityExpr(timeGroup.Granularity))
}
