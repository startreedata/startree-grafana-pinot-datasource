package plugin

import (
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/pinot-client-go/pinot"
	"strings"
	"time"
)

func GetColumnIdx(resultTable *pinot.ResultTable, colName string) (int, bool) {
	columnNames := resultTable.DataSchema.ColumnNames
	for idx := 0; idx < len(columnNames); idx++ {
		if columnNames[idx] == colName {
			return idx, true
		}
	}
	return -1, false
}

func GetTimeColumnFormat(ctx QueryContext, timeColumn string) (string, error) {
	for _, dtField := range ctx.TableSchema.DateTimeFieldSpecs {
		if dtField.Name == timeColumn {
			return dtField.Format, nil
		}
	}
	return "", fmt.Errorf("column `%s` is not a date time column", timeColumn)
}

func ExtractColumnToField(results *pinot.ResultTable, colIdx int) *data.Field {
	colName := results.DataSchema.ColumnNames[colIdx]
	return data.NewField(colName, nil, ExtractColumn(results, colIdx))
}

// ExtractColumn Extracts the column as a single array suitable for Grafana's data.Field.
func ExtractColumn(results *pinot.ResultTable, colIdx int) interface{} {
	colDataType := results.DataSchema.ColumnDataTypes[colIdx]
	switch colDataType {
	case "INT", "LONG":
		return ExtractTypedColumn[int64](results, colIdx, results.GetLong)
	case "FLOAT", "DOUBLE":
		return ExtractTypedColumn[float64](results, colIdx, results.GetDouble)
	case "STRING":
		return ExtractTypedColumn[string](results, colIdx, results.GetString)
	default:
		Logger.Error(fmt.Sprintf("column has unknown type %s", colDataType))
		return make([]int64, results.GetRowCount())
	}
}

func ExtractTypedColumn[V int64 | float64 | string](results *pinot.ResultTable, colIdx int, getter func(rowIdx, colIdx int) V) []V {
	values := make([]V, results.GetRowCount())
	for rowIdx := 0; rowIdx < results.GetRowCount(); rowIdx++ {
		values[rowIdx] = getter(rowIdx, colIdx)
	}
	return values
}

func ExtractTimeColumn(results *pinot.ResultTable, colIdx int, timeColumnFormat string) ([]time.Time, error) {
	if IsSimpleTimeColumnFormat(timeColumnFormat) {
		return ExtractSimpleDateTimeColumn(results, colIdx, timeColumnFormat)
	} else {
		return ExtractLongTimeColumn(results, colIdx, timeColumnFormat)
	}
}

func ExtractSimpleDateTimeColumn(results *pinot.ResultTable, colIdx int, timeColumnFormat string) ([]time.Time, error) {
	simpleDateTimeFormat, ok := SimpleDateTimeFormatFor(timeColumnFormat)
	if !ok {
		return nil, fmt.Errorf("invalid time column format: %s", timeColumnFormat)
	}

	values := make([]time.Time, results.GetRowCount())
	for i, val := range ExtractTypedColumn[string](results, colIdx, results.GetString) {
		ts, err := time.Parse(simpleDateTimeFormat, val)
		if err != nil {
			return nil, err
		}
		values[i] = ts
	}
	return values, nil
}

func ExtractLongTimeColumn(results *pinot.ResultTable, colIdx int, timeColumnFormat string) ([]time.Time, error) {
	timeConverter, ok := getLongTimeConverter(timeColumnFormat)
	if !ok {
		return nil, fmt.Errorf("invalid time column format: %s", timeColumnFormat)
	}
	values := make([]time.Time, results.GetRowCount())
	for i, val := range ExtractTypedColumn[int64](results, colIdx, results.GetLong) {
		values[i] = timeConverter(val)
	}
	return values, nil
}

func IsSimpleTimeColumnFormat(timeColumnFormat string) bool {
	return strings.HasPrefix(timeColumnFormat, "SIMPLE_DATE_FORMAT|")
}

func getLongTimeConverter(timeColumnFormat string) (func(v int64) time.Time, bool) {
	switch timeColumnFormat {
	case "EPOCH_NANOS", "1:NANOSECONDS:EPOCH", "EPOCH|NANOSECONDS", "EPOCH|NANOSECONDS|1":
		return func(v int64) time.Time { return time.Unix(0, v) }, true
	case "EPOCH_MICROS", "1:MICROSECONDS:EPOCH", "EPOCH|MICROSECONDS", "EPOCH|MICROSECONDS|1":
		return func(v int64) time.Time { return time.UnixMicro(v) }, true
	case "EPOCH_MILLIS", "1:MILLISECONDS:EPOCH", "EPOCH|MILLISECONDS", "EPOCH|MILLISECONDS|1", "EPOCH", "TIMESTAMP", "1:MILLISECONDS:TIMESTAMP":
		return func(v int64) time.Time { return time.UnixMilli(v) }, true
	case "EPOCH_SECONDS", "1:SECONDS:EPOCH", "EPOCH|SECONDS", "EPOCH|SECONDS|1":
		return func(v int64) time.Time { return time.Unix(v, 0) }, true
	case "EPOCH_MINUTES", "1:MINUTES:EPOCH", "EPOCH|MINUTES", "EPOCH|MINUTES|1":
		return func(v int64) time.Time { return time.Unix(v*60, 0) }, true
	case "EPOCH_HOURS", "1:HOURS:EPOCH", "EPOCH|HOURS", "EPOCH|HOURS|1":
		return func(v int64) time.Time { return time.Unix(v*3600, 0) }, true
	case "EPOCH_DAYS", "1:DAYS:EPOCH", "EPOCH|DAYS", "EPOCH|DAYS|1":
		return func(v int64) time.Time { return time.Unix(v*86400, 0) }, true
	default:
		return nil, false
	}
}
