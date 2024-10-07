package pinotlib

import (
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/pinot-client-go/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/logger"
	"math"
	"strconv"
	"strings"
	"time"
)

func GetColumnIdx(resultTable *pinot.ResultTable, colName string) (int, error) {
	for idx := range resultTable.DataSchema.ColumnNames {
		if resultTable.DataSchema.ColumnNames[idx] == colName {
			return idx, nil
		}
	}
	return -1, fmt.Errorf("column %s not found", colName)
}

func GetTimeColumnFormat(tableSchema TableSchema, timeColumn string) (string, error) {
	for _, dtField := range tableSchema.DateTimeFieldSpecs {
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

// ExtractColumn extracts a column from the table.
// The column data type is mapped to the corresponding golang type.
func ExtractColumn(results *pinot.ResultTable, colIdx int) interface{} {
	colDataType := results.DataSchema.ColumnDataTypes[colIdx]
	// TODO: Handle TIMESTAMP data type.
	switch colDataType {
	case DataTypeBoolean:
		return extractTypedColumn[bool](results, colIdx, func(rowIdx, _ int) bool {
			return (results.Get(rowIdx, colIdx)).(bool)
		})
	case DataTypeInt, DataTypeLong:
		return extractTypedColumn[int64](results, colIdx, results.GetLong)
	case DataTypeFloat, DataTypeDouble:
		return extractTypedColumn[float64](results, colIdx, results.GetDouble)
	case DataTypeString, DataTypeJson, DataTypeBytes:
		return extractTypedColumn[string](results, colIdx, results.GetString)
	case DataTypeTimestamp:
		return extractTypedColumn[time.Time](results, colIdx, func(rowIdx, _ int) time.Time {
			var (
				year   int
				month  time.Month
				day    int
				hour   int
				minute int
				second float64
			)
			_, _ = fmt.Sscanf(results.GetString(rowIdx, colIdx), "%d-%d-%d %d:%d:%f", &year, &month, &day, &hour, &minute, &second)
			_, fractional := math.Modf(second)
			return time.Date(year, month, day, hour, minute, int(second), int(fractional*float64(time.Second)), time.UTC)
		})
	default:
		logger.Logger.Error(fmt.Sprintf("column has unknown type %s", colDataType))
		return make([]int64, results.GetRowCount())
	}
}

func extractTypedColumn[V int64 | float64 | string | bool | time.Time](results *pinot.ResultTable, colIdx int, getter func(rowIdx, colIdx int) V) []V {
	values := make([]V, results.GetRowCount())
	for rowIdx := 0; rowIdx < results.GetRowCount(); rowIdx++ {
		values[rowIdx] = getter(rowIdx, colIdx)
	}
	return values
}

func ExtractLongColumn(results *pinot.ResultTable, colIdx int) []int64 {
	switch rawVals := ExtractColumn(results, colIdx).(type) {
	case []int64:
		return rawVals
	case []float64:
		vals := make([]int64, results.GetRowCount())
		for i := range rawVals {
			vals[i] = int64(rawVals[i])
		}
		return vals
	case []bool:
		vals := make([]int64, results.GetRowCount())
		for i := range rawVals {
			if rawVals[i] {
				vals[i] = 1
			}
		}
		return vals
	default:
		return make([]int64, results.GetRowCount())
	}
}

func ExtractDoubleColumn(results *pinot.ResultTable, colIdx int) []float64 {
	switch rawVals := ExtractColumn(results, colIdx).(type) {
	case []int64:
		vals := make([]float64, results.GetRowCount())
		for i := range rawVals {
			vals[i] = float64(rawVals[i])
		}
		return vals
	case []float64:
		return rawVals
	case []bool:
		vals := make([]float64, results.GetRowCount())
		for i := range rawVals {
			if rawVals[i] {
				vals[i] = 1
			}
		}
		return vals
	default:
		return make([]float64, results.GetRowCount())
	}
}

func ExtractBooleanColumn(results *pinot.ResultTable, colIdx int) []bool {
	switch rawVals := ExtractColumn(results, colIdx).(type) {
	case []int64:
		vals := make([]bool, results.GetRowCount())
		for i := range rawVals {
			vals[i] = rawVals[i] != 0
		}
		return vals
	case []float64:
		vals := make([]bool, results.GetRowCount())
		for i := range rawVals {
			vals[i] = rawVals[i] != 0
		}
		return vals
	case []bool:
		return rawVals
	case []string:
		vals := make([]bool, results.GetRowCount())
		for i := range rawVals {
			vals[i], _ = strconv.ParseBool(rawVals[i])
		}
		return vals
	default:
		return make([]bool, results.GetRowCount())
	}
}

func ExtractStringColumn(results *pinot.ResultTable, colIdx int) []string {
	switch rawVals := ExtractColumn(results, colIdx).(type) {
	case []int64:
		vals := make([]string, results.GetRowCount())
		for i := range rawVals {
			vals[i] = fmt.Sprintf("%d", rawVals[i])
		}
		return vals
	case []float64:
		vals := make([]string, results.GetRowCount())
		for i := range rawVals {
			vals[i] = fmt.Sprintf("%v", rawVals[i])
		}
		return vals
	case []bool:
		vals := make([]string, results.GetRowCount())
		for i := range rawVals {
			vals[i] = fmt.Sprintf("%v", rawVals[i])
		}
		return vals
	case []string:
		return rawVals
	default:
		return make([]string, results.GetRowCount())
	}
}

func ExtractJsonColumn[V any](results *pinot.ResultTable, colIdx int) ([]V, error) {
	values := make([]V, results.GetRowCount())
	for i, jsonStr := range ExtractStringColumn(results, colIdx) {
		if err := json.Unmarshal([]byte(jsonStr), &values[i]); err != nil {
			return nil, fmt.Errorf("failed to unmarshal json at row %d, column %d: %v", i, colIdx, err)
		}
	}
	return values, nil
}

// ExtractColumnExpr extracts the column as a slice of sql expressions representing the column value.
// Strings will be single-quoted. Numbers and booleans are unquoted.
func ExtractColumnExpr(results *pinot.ResultTable, colIdx int) []string {
	exprs := make([]string, results.GetRowCount())
	switch rawVals := ExtractColumn(results, colIdx).(type) {
	case []int64:
		for i := range rawVals {
			exprs[i] = fmt.Sprintf("%d", rawVals[i])
		}
	case []float64:
		for i := range rawVals {
			exprs[i] = fmt.Sprintf("%v", rawVals[i])
		}
	case []bool:
		for i := range rawVals {
			exprs[i] = fmt.Sprintf("%v", rawVals[i])
		}
	case []string:
		for i := range rawVals {
			exprs[i] = fmt.Sprintf("'%s'", rawVals[i])
		}
	}
	return exprs
}

//func ExtractTimestampColumn(results *pinot.ResultTable, colIdx int) []string {
//
//}

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
	for i, val := range extractTypedColumn[string](results, colIdx, results.GetString) {
		ts, err := time.Parse(simpleDateTimeFormat, val)
		if err != nil {
			return nil, err
		}
		values[i] = ts
	}
	return values, nil
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

func ExtractLongTimeColumn(results *pinot.ResultTable, colIdx int, timeColumnFormat string) ([]time.Time, error) {
	timeConverter, ok := getLongTimeConverter(timeColumnFormat)
	if !ok {
		return nil, fmt.Errorf("invalid time column format: %s", timeColumnFormat)
	}
	values := make([]time.Time, results.GetRowCount())
	for i, val := range ExtractLongColumn(results, colIdx) {
		values[i] = timeConverter(int64(val))
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
	default:
		return nil, false
	}
}

func GetDistinctValues[T comparable](vals []T) []T {
	observed := make(map[T]struct{})
	var result []T
	for _, val := range vals {
		if _, ok := observed[val]; !ok {
			result = append(result, val)
			observed[val] = struct{}{}
		}
	}
	return result[:]
}
