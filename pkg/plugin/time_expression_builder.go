package plugin

import (
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/pinot-client-go/pinot"
	"regexp"
	"strings"
	"time"
)

var simpleDateFormatPattern = regexp.MustCompile(`^([0-9]:[A-Z]+:)?SIMPLE_DATE_FORMAT:`)

var timeExpressionBuilderCache = make(map[string]timeExprFormat)

func ResetTimeExpressionBuilderCache() {
	timeExpressionBuilderCache = make(map[string]timeExprFormat)
}

type TimeExpressionBuilder struct {
	timeColumn       string
	timeColumnFormat string
	timeExprFormat
}

type timeExprFormat struct {
	inputFormat  string
	encodeTime   func(time.Time) string
	decodeLong   func(v int64) time.Time
	decodeString func(v string) (time.Time, error)
}

func TimeExpressionBuilderFor(queryCtx QueryContext, timeColumn string) (TimeExpressionBuilder, error) {
	timeColumn = strings.Trim(timeColumn, "\"`")
	if len(timeColumn) == 0 {
		return TimeExpressionBuilder{}, fmt.Errorf("timeColumn cannot be empty")
	}

	timeColumnFormat, ok := timeColumnFormatFor(queryCtx, timeColumn)
	if !ok {
		return TimeExpressionBuilder{}, fmt.Errorf("column `%s` is not a date time column", timeColumn)
	}

	populateAndCache := func(format timeExprFormat) TimeExpressionBuilder {
		timeExpressionBuilderCache[timeColumnFormat] = format
		return TimeExpressionBuilder{
			timeColumn:       timeColumn,
			timeColumnFormat: timeColumnFormat,
			timeExprFormat:   format,
		}
	}

	if format, ok := timeExpressionBuilderCache[timeColumnFormat]; ok {
		return populateAndCache(format), nil
	}

	format, ok := timeExprFormatFor(timeColumn, timeColumnFormat)
	if !ok {
		return TimeExpressionBuilder{}, fmt.Errorf("column `%s` has unsupported time format `%s`", timeColumn, timeColumnFormat)
	}
	return populateAndCache(format), nil
}

func (x TimeExpressionBuilder) BuildTimeFilterExpr(timeRange backend.TimeRange) string {
	return fmt.Sprintf(`"%s" >= %s AND "%s" <= %s`,
		x.timeColumn, x.encodeTime(timeRange.From),
		x.timeColumn, x.encodeTime(timeRange.To),
	)
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

	return fmt.Sprintf(` DATETIMECONVERT("%s", '%s', '1:MILLISECONDS:EPOCH', '%s') `,
		x.timeColumn, x.inputFormat, granularity)
}

func (x TimeExpressionBuilder) ExtractTimeColumn(results *pinot.ResultTable) ([]time.Time, error) {
	colIdx := -1
	for i, colName := range results.DataSchema.ColumnNames {
		if colName == x.timeColumn {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		return nil, fmt.Errorf("column %s not in result schema", x.timeColumn)
	}
	// TODO This can result in null if the time column format expects different data type
	if results.DataSchema.ColumnDataTypes[colIdx] == "STRING" {
		return x.extractTimeColumnFromString(results, colIdx)
	} else {
		return x.extractTimeColumnFromLong(results, colIdx), nil
	}
}

func (x TimeExpressionBuilder) extractTimeColumnFromLong(results *pinot.ResultTable, colIdx int) []time.Time {
	values := make([]time.Time, results.GetRowCount())
	for rowIdx := 0; rowIdx < results.GetRowCount(); rowIdx++ {
		values[rowIdx] = x.decodeLong(results.GetLong(rowIdx, colIdx))
	}
	return values
}

func (x TimeExpressionBuilder) extractTimeColumnFromString(results *pinot.ResultTable, colIdx int) ([]time.Time, error) {
	values := make([]time.Time, results.GetRowCount())
	for rowIdx := 0; rowIdx < results.GetRowCount(); rowIdx++ {
		res, err := x.decodeString(results.GetString(rowIdx, colIdx))
		if err != nil {
			return nil, err
		}
		values[rowIdx] = res
	}
	return values, nil
}

func timeColumnFormatFor(ctx QueryContext, timeColumn string) (string, bool) {
	var timeColumnFormat string
	var ok bool
	for _, dtField := range ctx.TableSchema.DateTimeFieldSpecs {
		if dtField.Name == timeColumn {
			timeColumnFormat = dtField.Format
			ok = true
		}
	}
	return timeColumnFormat, ok
}

func timeExprFormatFor(timeColumn string, timeColumnFormat string) (timeExprFormat, bool) {
	switch timeColumnFormat {
	case "EPOCH_NANOS", "1:NANOSECONDS:EPOCH", "EPOCH|NANOSECONDS", "EPOCH|NANOSECONDS|1":
		return timeExprFormat{
			inputFormat: "1:NANOSECONDS:EPOCH",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.UnixNano()) },
			decodeLong:  func(v int64) time.Time { return time.Unix(0, v) },
		}, true

	case "EPOCH_MICROS", "1:MICROSECONDS:EPOCH", "EPOCH|MICROSECONDS", "EPOCH|MICROSECONDS|1":
		return timeExprFormat{
			inputFormat: "1:MICROSECONDS:EPOCH",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.UnixMicro()) },
			decodeLong:  func(v int64) time.Time { return time.UnixMicro(v) },
		}, true
	case "EPOCH_MILLIS", "1:MILLISECONDS:EPOCH", "EPOCH|MILLISECONDS", "EPOCH|MILLISECONDS|1", "EPOCH":
		return timeExprFormat{
			inputFormat: "1:MILLISECONDS:EPOCH",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.UnixMilli()) },
			decodeLong:  func(v int64) time.Time { return time.UnixMilli(v) },
		}, true
	case "TIMESTAMP", "1:MILLISECONDS:TIMESTAMP":
		return timeExprFormat{
			inputFormat: "1:MILLISECONDS:TIMESTAMP",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.UnixMilli()) },
			decodeLong:  func(v int64) time.Time { return time.UnixMilli(v) },
		}, true
	case "EPOCH_SECONDS", "1:SECONDS:EPOCH", "EPOCH|SECONDS", "EPOCH|SECONDS|1":
		return timeExprFormat{
			inputFormat: "1:SECONDS:EPOCH",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.Unix()) },
			decodeLong:  func(v int64) time.Time { return time.Unix(v, 0) },
		}, true
	case "EPOCH_MINUTES", "1:MINUTES:EPOCH", "EPOCH|MINUTES", "EPOCH|MINUTES|1":
		return timeExprFormat{
			inputFormat: "1:MINUTES:EPOCH",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.Unix()/60) },
			decodeLong:  func(v int64) time.Time { return time.Unix(v*60, 0) },
		}, true
	case "EPOCH_HOURS", "1:HOURS:EPOCH", "EPOCH|HOURS", "EPOCH|HOURS|1":
		return timeExprFormat{
			inputFormat: "1:HOURS:EPOCH",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.Unix()/3600) },
			decodeLong:  func(v int64) time.Time { return time.Unix(v*3600, 0) },
		}, true
	case "EPOCH_DAYS", "1:DAYS:EPOCH", "EPOCH|DAYS", "EPOCH|DAYS|1":
		return timeExprFormat{
			inputFormat: "1:DAYS:EPOCH",
			encodeTime:  func(d time.Time) string { return fmt.Sprintf("%d", d.Unix()/86400) },
			decodeLong:  func(v int64) time.Time { return time.Unix(v*86400, 0) },
		}, true
	}

	var sdfPattern string
	if strings.HasPrefix(timeColumnFormat, "SIMPLE_DATE_FORMAT|") {
		sdfElements := strings.Split(timeColumnFormat, "|")
		if len(sdfElements) >= 2 {
			sdfPattern = sdfElements[1]
		} else {
			backend.Logger.Error("pinot-datasource: invalid time format: %s", timeColumnFormat)
			return timeExprFormat{}, false
		}
	}

	if _, err := time.Parse(sdfPattern, time.Now().Format(sdfPattern)); err != nil {
		backend.Logger.Error("pinot-datasource: failed to interpret time column format: %s", sdfPattern)
		return timeExprFormat{}, false
	}

	return timeExprFormat{
		inputFormat:  "1:DAYS:SIMPLE_DATE_FORMAT:" + sdfPattern,
		encodeTime:   func(d time.Time) string { return fmt.Sprintf(`"%s"`, d.Format(sdfPattern)) },
		decodeString: func(v string) (time.Time, error) { return time.Parse(sdfPattern, v) },
	}, true
}
