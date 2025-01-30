package pinotlib

import (
	"fmt"
	"strings"
	"time"
)

// SqlExpr is a snippet of valid Pinot SQL.
// SqlExprs can be stitched together to form an entire SQL statement.
// SqlExprs are different from normal strings that may not have been properly quoted or encoded for SQL.
type SqlExpr string

func (x SqlExpr) String() string { return string(x) }

func ObjectExpr(obj string) SqlExpr {
	return SqlExpr(fmt.Sprintf(`"%s"`, obj))
}

func StringLiteralExpr(lit string) SqlExpr {
	return SqlExpr(fmt.Sprintf(`'%s'`, lit))
}

func LiteralExpr[T string | int | int64 | int32 | bool | float32 | float64](val T) SqlExpr {
	switch valTyped := any(val).(type) {
	case bool:
		if valTyped {
			return "TRUE"
		} else {
			return "FALSE"
		}
	case int, int64, int32:
		return SqlExpr(fmt.Sprintf("%d", valTyped))
	case float32, float64:
		return SqlExpr(fmt.Sprintf("%v", valTyped))
	default:
		return SqlExpr(fmt.Sprintf(`'%s'`, valTyped))
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

func ComplexFieldExpr(column string, key string) SqlExpr {
	if key == "" {
		return ObjectExpr(column)
	} else {
		return SqlExpr(fmt.Sprintf("%s[%s]", ObjectExpr(column), StringLiteralExpr(key)))
	}
}

type TimeFilter struct {
	Column string
	Format DateTimeFormat
	From   time.Time
	To     time.Time
}

func TimeFilterBucketAlignedExpr(filter TimeFilter, bucketSize time.Duration) SqlExpr {
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

func TimeFilterExpr(filter TimeFilter) SqlExpr {
	return SqlExpr(fmt.Sprintf(`%s >= %s AND %s < %s`,
		ObjectExpr(filter.Column), TimeExpr(filter.From, filter.Format),
		ObjectExpr(filter.Column), TimeExpr(filter.To, filter.Format),
	))
}

func TimeExpr(ts time.Time, format DateTimeFormat) SqlExpr {
	switch format.Unit {
	case TimeUnitNanoseconds:
		return SqlExpr(fmt.Sprintf("%d", ts.UnixNano()/int64(format.Size)))
	case TimeUnitMicroseconds:
		return SqlExpr(fmt.Sprintf("%d", ts.UnixMicro()/int64(format.Size)))
	case TimeUnitMilliseconds:
		return SqlExpr(fmt.Sprintf("%d", ts.UnixMilli()/int64(format.Size)))
	case TimeUnitSeconds:
		return SqlExpr(fmt.Sprintf("%d", ts.Unix()/int64(format.Size)))
	case TimeUnitMinutes:
		return SqlExpr(fmt.Sprintf("%d", ts.Unix()/int64(format.Size)/60))
	case TimeUnitHours:
		return SqlExpr(fmt.Sprintf("%d", ts.Unix()/int64(format.Size)/3600))
	case TimeUnitDays:
		return SqlExpr(fmt.Sprintf("%d", ts.Unix()/int64(format.Size)/86400))
	default:
		return ""
	}
}

func GranularityExpr(granularity Granularity) SqlExpr {
	return StringLiteralExpr(granularity.String())
}

func DateTimeFormatExpr(format DateTimeFormat) SqlExpr {
	return StringLiteralExpr(format.LegacyString())
}

func TimeGroupExpr(configs ListTableConfigsResponse, timeGroup DateTimeConversion) SqlExpr {
	if timeGroup.Granularity.Duration() == timeGroup.InputFormat.MinimumGranularity().Duration() &&
		timeGroup.InputFormat.Equals(timeGroup.OutputFormat) {
		return ObjectExpr(timeGroup.TimeColumn)
	}

	if timeCol, ok := DerivedTimeColumnFor(configs, timeGroup); ok {
		return ObjectExpr(timeCol)
	}

	return SqlExpr(fmt.Sprintf(`DATETIMECONVERT(%s, %s, %s, %s)`,
		ObjectExpr(timeGroup.TimeColumn),
		DateTimeFormatExpr(timeGroup.InputFormat),
		DateTimeFormatExpr(timeGroup.OutputFormat),
		GranularityExpr(timeGroup.Granularity)))
}

func JsonExtractScalarExpr(sourceExpr SqlExpr, path string, resultType string, defaultValueExpr SqlExpr) SqlExpr {
	return SqlExpr(fmt.Sprintf(`JSONEXTRACTSCALAR(%s, %s, %s, %s)`,
		sourceExpr, StringLiteralExpr(path), StringLiteralExpr(resultType), defaultValueExpr))
}

func RegexpExtractExpr(sourceExpr SqlExpr, pattern string, group int, defaultValueExpr SqlExpr) SqlExpr {
	return SqlExpr(fmt.Sprintf(`REGEXPEXTRACT(%s, %s, %d, %s)`,
		sourceExpr, StringLiteralExpr(pattern), group, defaultValueExpr))
}

func QueryOptionExpr(name string, valueExpr SqlExpr) SqlExpr {
	return SqlExpr(fmt.Sprintf(`SET %s=%s;`, name, valueExpr))
}

func OrderByExpr(columnExpr SqlExpr, direction string) SqlExpr {
	if strings.ToUpper(direction) == "DESC" {
		direction = "DESC"
	} else {
		direction = "ASC"
	}
	return SqlExpr(fmt.Sprintf(`%s %s`, columnExpr, direction))
}

func CastExpr(columnExpr SqlExpr, dataType string) SqlExpr {
	return SqlExpr(fmt.Sprintf(`CAST(%s AS %s)`, columnExpr, dataType))
}
