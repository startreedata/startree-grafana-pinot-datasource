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

func LiteralExpr[T string | int | int64 | int32 | bool | float32 | float64](val T) string {
	switch valTyped := any(val).(type) {
	case bool:
		if valTyped {
			return "TRUE"
		} else {
			return "FALSE"
		}
	case int, int64, int32:
		return fmt.Sprintf("%d", valTyped)
	case float32, float64:
		return fmt.Sprintf("%v", valTyped)
	default:
		return fmt.Sprintf(`'%s'`, valTyped)
	}
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

func ComplexFieldExpr(column string, key string) string {
	if key == "" {
		return ObjectExpr(column)
	} else {
		return fmt.Sprintf("%s[%s]", ObjectExpr(column), StringLiteralExpr(key))
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
	if timeGroup.Granularity.Duration() == timeGroup.InputFormat.MinimumGranularity().Duration() &&
		timeGroup.InputFormat.Equals(timeGroup.OutputFormat) {
		return ObjectExpr(timeGroup.TimeColumn)
	}

	if timeCol, ok := DerivedTimeColumnFor(configs, timeGroup); ok {
		return ObjectExpr(timeCol)
	}

	return fmt.Sprintf(`DATETIMECONVERT(%s, %s, %s, %s)`,
		ObjectExpr(timeGroup.TimeColumn),
		DateTimeFormatExpr(timeGroup.InputFormat),
		DateTimeFormatExpr(timeGroup.OutputFormat),
		GranularityExpr(timeGroup.Granularity))
}

func JsonExtractScalarExpr(sourceExpr string, path string, resultType string, defaultValueExpr string) string {
	return fmt.Sprintf(`JSONEXTRACTSCALAR(%s, %s, %s, %s)`,
		sourceExpr, StringLiteralExpr(path), StringLiteralExpr(resultType), defaultValueExpr)
}

func RegexpExtractExpr(sourceExpr string, pattern string, group int, defaultValueExpr string) string {
	return fmt.Sprintf(`REGEXPEXTRACT(%s, %s, %d, %s)`,
		sourceExpr, StringLiteralExpr(pattern), group, defaultValueExpr)
}

func QueryOptionExpr(name string, valueExpr string) string {
	return fmt.Sprintf(`SET %s=%s;`, name, valueExpr)
}

func OrderByExpr(columnExpr string, direction string) string {
	if strings.ToUpper(direction) == "DESC" {
		direction = "DESC"
	} else {
		direction = "ASC"
	}
	return fmt.Sprintf(`%s %s`, columnExpr, direction)
}
