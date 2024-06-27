package plugin

import (
	"fmt"
	"strings"
	"time"
)

const TimeGroupExprOutputFormat = "1:MILLISECONDS:EPOCH"

type TimeExpressionBuilder struct {
	timeColumn       string
	timeColumnFormat string
	timeExprFormat   TimeExprFormat
}

func TimeExpressionBuilderFor(tableSchema TableSchema, timeColumn string) (TimeExpressionBuilder, error) {
	timeColumn = strings.Trim(timeColumn, "\"`")
	if len(timeColumn) == 0 {
		return TimeExpressionBuilder{}, fmt.Errorf("timeColumn cannot be empty")
	}

	timeColumnFormat, err := GetTimeColumnFormat(tableSchema, timeColumn)
	if err != nil {
		return TimeExpressionBuilder{}, err
	}

	exprFormat, ok := NewTimeExprFormat(timeColumnFormat)
	if !ok {
		return TimeExpressionBuilder{}, fmt.Errorf("time column `%s` has unsupported format `%s`", timeColumn, timeColumnFormat)
	}

	return TimeExpressionBuilder{
		timeColumn:       timeColumn,
		timeColumnFormat: timeColumnFormat,
		timeExprFormat:   exprFormat,
	}, nil
}

func (x TimeExpressionBuilder) BuildTimeFilterExpr(timeRange TimeRange) string {
	return fmt.Sprintf(`"%s" >= %s AND "%s" <= %s`,
		x.timeColumn, x.TimeExpr(timeRange.From),
		x.timeColumn, x.TimeExpr(timeRange.To),
	)
}

func (x TimeExpressionBuilder) TimeExpr(ts time.Time) string {
	return x.timeExprFormat.encodeTime(ts)
}

func (x TimeExpressionBuilder) BuildTimeGroupExpr(bucketSize time.Duration) string {
	var granularity string
	switch {
	case bucketSize.Hours() >= 1:
		granularity = fmt.Sprintf("%d:HOURS", int(bucketSize.Hours()))
	case bucketSize.Minutes() >= 1:
		granularity = fmt.Sprintf("%d:MINUTES", int(bucketSize.Minutes()))
	case bucketSize.Seconds() >= 1:
		granularity = fmt.Sprintf("%d:SECONDS", int(bucketSize.Seconds()))
	case bucketSize.Milliseconds() >= 1:
		granularity = fmt.Sprintf("%d:MILLISECONDS", int(bucketSize.Milliseconds()))
	case bucketSize.Microseconds() >= 1:
		granularity = fmt.Sprintf("%d:MICROSECONDS", int(bucketSize.Microseconds()))
	default:
		granularity = fmt.Sprintf("%d:NANOSECONDS", int(bucketSize.Nanoseconds()))
	}

	return fmt.Sprintf(`DATETIMECONVERT("%s", '%s', '%s', '%s')`,
		x.timeColumn, x.timeExprFormat.inputFormat, TimeGroupExprOutputFormat, granularity)
}

type TimeExprFormat struct {
	inputFormat string
	encodeTime  func(d time.Time) string
}

func NewTimeExprFormat(timeColumnFormat string) (TimeExprFormat, bool) {
	switch timeColumnFormat {
	case "EPOCH_NANOS", "1:NANOSECONDS:EPOCH", "EPOCH|NANOSECONDS", "EPOCH|NANOSECONDS|1":
		return TimeExprFormat{
			inputFormat: "1:NANOSECONDS:EPOCH",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.UnixNano()) },
		}, true
	case "EPOCH_MICROS", "1:MICROSECONDS:EPOCH", "EPOCH|MICROSECONDS", "EPOCH|MICROSECONDS|1":
		return TimeExprFormat{
			inputFormat: "1:MICROSECONDS:EPOCH",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.UnixMicro()) },
		}, true
	case "EPOCH_MILLIS", "1:MILLISECONDS:EPOCH", "EPOCH|MILLISECONDS", "EPOCH|MILLISECONDS|1", "EPOCH":
		return TimeExprFormat{
			inputFormat: "1:MILLISECONDS:EPOCH",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.UnixMilli()) },
		}, true
	case "TIMESTAMP", "1:MILLISECONDS:TIMESTAMP":
		return TimeExprFormat{
			inputFormat: "1:MILLISECONDS:TIMESTAMP",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.UnixMilli()) },
		}, true
	case "EPOCH_SECONDS", "1:SECONDS:EPOCH", "EPOCH|SECONDS", "EPOCH|SECONDS|1":
		return TimeExprFormat{
			inputFormat: "1:SECONDS:EPOCH",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.Unix()) },
		}, true
	case "EPOCH_MINUTES", "1:MINUTES:EPOCH", "EPOCH|MINUTES", "EPOCH|MINUTES|1":
		return TimeExprFormat{
			inputFormat: "1:MINUTES:EPOCH",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.Unix()/60) },
		}, true
	case "EPOCH_HOURS", "1:HOURS:EPOCH", "EPOCH|HOURS", "EPOCH|HOURS|1":
		return TimeExprFormat{
			inputFormat: "1:HOURS:EPOCH",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.Unix()/3600) },
		}, true
	case "EPOCH_DAYS", "1:DAYS:EPOCH", "EPOCH|DAYS", "EPOCH|DAYS|1":
		return TimeExprFormat{
			inputFormat: "1:DAYS:EPOCH",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.Unix()/86400) },
		}, true
	}

	sdfPattern, ok := SimpleDateTimeFormatFor(timeColumnFormat)
	if !ok {
		return TimeExprFormat{}, false
	}

	return TimeExprFormat{
		inputFormat: "1:DAYS:SIMPLE_DATE_FORMAT:" + sdfPattern,
		encodeTime:  func(d time.Time) string { return fmt.Sprintf(`'%s'`, d.Format(sdfPattern)) },
	}, true
}

func SimpleDateTimeFormatFor(timeColumnFormat string) (string, bool) {
	if !IsSimpleTimeColumnFormat(timeColumnFormat) {
		return "", false
	}

	sdfElements := strings.Split(timeColumnFormat, "|")
	if len(sdfElements) < 2 {
		return "", false
	}
	sdfPattern := sdfElements[1]

	if _, err := time.Parse(sdfPattern, time.Now().Format(sdfPattern)); err != nil {
		return "", false
	}
	return sdfPattern, true
}
