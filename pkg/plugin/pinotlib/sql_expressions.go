package pinotlib

import (
	"fmt"
	"time"
)

const (
	FormatMillisecondsEpoch = "1:MILLISECONDS:EPOCH"
)

func SqlObjectExpr(obj string) string {
	return fmt.Sprintf(`"%s"`, obj)
}

func SqlLiteralStringExpr(lit string) string {
	return fmt.Sprintf(`'%s'`, lit)
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
		SqlObjectExpr(filter.Column), TimeExpr(filter.From, filter.Format),
		SqlObjectExpr(filter.Column), TimeExpr(filter.To, filter.Format),
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
	return SqlLiteralStringExpr(granularity.String())
}

func DateTimeFormatExpr(format DateTimeFormat) string {
	return SqlLiteralStringExpr(format.LegacyString())
}

func TimeGroupExpr(config TableConfig, timeGroup DateTimeConversion) string {
	derivedColumns := DerivedTimeColumnsFrom(config)
	for _, col := range derivedColumns {
		if col.Source.Equals(timeGroup) {
			return SqlObjectExpr(col.ColumnName)
		}
	}
	return fmt.Sprintf(`DATETIMECONVERT(%s, %s, %s, %s)`,
		SqlObjectExpr(timeGroup.TimeColumn),
		DateTimeFormatExpr(timeGroup.InputFormat),
		DateTimeFormatExpr(timeGroup.OutputFormat),
		GranularityExpr(timeGroup.Granularity))
}
