package pinotlib

import (
	"fmt"
	"time"
)

const (
	FormatMillisecondsEpoch   = "1:MILLISECONDS:EPOCH"
	TimeGroupExprOutputFormat = FormatMillisecondsEpoch
)

func SqlObjectExpr(obj string) string {
	return fmt.Sprintf(`"%s"`, obj)
}

func SqlLiteralStringExpr(obj string) string {
	return fmt.Sprintf(`'%s'`, obj)
}

type TimeExpressionBuilder struct {
	timeColumn       string
	timeColumnFormat string
	format           PinotDateTimeFormat
}

func TimeExpressionBuilderFor(tableSchema TableSchema, timeColumn string) (TimeExpressionBuilder, error) {
	if len(timeColumn) == 0 {
		return TimeExpressionBuilder{}, fmt.Errorf("time column cannot be empty")
	}

	timeColumnFormat, err := GetTimeColumnFormat(tableSchema, timeColumn)
	if err != nil {
		return TimeExpressionBuilder{}, err
	}

	return NewTimeExpressionBuilder(timeColumn, timeColumnFormat)
}

func NewTimeExpressionBuilder(timeColumn string, timeColumnFormat string) (TimeExpressionBuilder, error) {
	format, err := ParsePinotDateTimeFormat(timeColumnFormat)
	if err != nil {
		return TimeExpressionBuilder{}, fmt.Errorf("failed to parse date time format `%s`: %w", timeColumnFormat, err)
	}

	return TimeExpressionBuilder{
		timeColumn:       timeColumn,
		timeColumnFormat: timeColumnFormat,
		format:           format,
	}, nil
}

func (x TimeExpressionBuilder) TimeColumnFormat() string {
	return x.timeColumnFormat
}

func (x TimeExpressionBuilder) TimeFilterBucketAlignedExpr(from time.Time, to time.Time, bucketSize time.Duration) string {
	fromTrunc := from.Truncate(bucketSize)
	toTrunc := to.Truncate(bucketSize)
	if toTrunc.Before(to) {
		toTrunc = toTrunc.Add(bucketSize)
	}

	return x.TimeFilterExpr(fromTrunc, toTrunc)
}

func (x TimeExpressionBuilder) TimeFilterExpr(from time.Time, to time.Time) string {
	return fmt.Sprintf(`%s >= %s AND %s < %s`,
		SqlObjectExpr(x.timeColumn), x.TimeExpr(from),
		SqlObjectExpr(x.timeColumn), x.TimeExpr(to),
	)
}

func (x TimeExpressionBuilder) TimeExpr(ts time.Time) string {
	return x.format.FormatTime(ts)
}

func (x TimeExpressionBuilder) TimeGroupExpr(granularity string) string {
	return fmt.Sprintf(`DATETIMECONVERT("%s", '%s', '%s', '%s')`,
		x.timeColumn, x.format.LegacyString(), TimeGroupExprOutputFormat, granularity)
}
