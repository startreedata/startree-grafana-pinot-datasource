package pinotlib

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"math"
	"math/big"
	"time"
)

// https://docs.pinot.apache.org/configuration-reference/schema

const (
	DataTypeInt        = "INT"
	DataTypeLong       = "LONG"
	DataTypeFloat      = "FLOAT"
	DataTypeDouble     = "DOUBLE"
	DataTypeBoolean    = "BOOLEAN"
	DataTypeTimestamp  = "TIMESTAMP"
	DataTypeString     = "STRING"
	DataTypeJson       = "JSON"
	DataTypeBytes      = "BYTES"
	DataTypeBigDecimal = "BIG_DECIMAL"
	DataTypeMap        = "MAP"
)

func GetColumnName(resultTable *ResultTable, colIdx int) (string, error) {
	if colIdx > len(resultTable.DataSchema.ColumnNames) {
		return "", fmt.Errorf("column index %d out of range", colIdx)
	}
	return resultTable.DataSchema.ColumnNames[colIdx], nil
}

func GetColumnIdx(resultTable *ResultTable, colName string) (int, error) {
	for idx := range resultTable.DataSchema.ColumnNames {
		if resultTable.DataSchema.ColumnNames[idx] == colName {
			return idx, nil
		}
	}
	return -1, fmt.Errorf("column %s not found", colName)
}

func GetTimeColumnFormat(tableSchema TableSchema, timeColumn string) (DateTimeFormat, error) {
	for _, dtField := range tableSchema.DateTimeFieldSpecs {
		if dtField.Name == timeColumn {
			return ParseDateTimeFormat(dtField.Format)
		}
	}
	return DateTimeFormat{}, fmt.Errorf("column `%s` is not a date time column", timeColumn)
}

// ExtractColumn extracts a column from the table.
// The column data type is mapped to the corresponding golang type.
func ExtractColumn(results *ResultTable, colIdx int) interface{} {
	colDataType := results.DataSchema.ColumnDataTypes[colIdx]
	switch colDataType {
	case DataTypeBoolean:
		return extractTypedColumn(results, func(rowIdx int) (bool, error) {
			return (results.Rows[rowIdx][colIdx]).(bool), nil
		})
	case DataTypeInt:
		return extractTypedColumn(results, func(rowIdx int) (int32, error) {
			val, err := (results.Rows[rowIdx][colIdx]).(json.Number).Int64()
			return int32(val), err
		})
	case DataTypeLong:
		return extractTypedColumn(results, func(rowIdx int) (int64, error) {
			return (results.Rows[rowIdx][colIdx]).(json.Number).Int64()
		})
	case DataTypeFloat:
		return extractTypedColumn(results, func(rowIdx int) (float32, error) {
			val, err := extractDouble(results.Rows[rowIdx][colIdx])
			return float32(val), err
		})
	case DataTypeDouble:
		return extractTypedColumn(results, func(rowIdx int) (float64, error) {
			return extractDouble(results.Rows[rowIdx][colIdx])
		})
	case DataTypeBigDecimal:
		// ref: https://github.com/apache/pinot/issues/8418
		return extractTypedColumn(results, func(rowIdx int) (*big.Int, error) {
			var val big.Int
			return &val, val.UnmarshalText([]byte(results.Rows[rowIdx][colIdx].(string)))
		})
	case DataTypeString:
		return extractTypedColumn(results, func(rowIdx int) (string, error) {
			return results.Rows[rowIdx][colIdx].(string), nil
		})
	case DataTypeBytes:
		return extractTypedColumn(results, func(rowIdx int) ([]byte, error) {
			return hex.DecodeString(results.Rows[rowIdx][colIdx].(string))
		})
	case DataTypeJson:
		return extractTypedColumn(results, func(rowIdx int) (json.RawMessage, error) {
			return json.RawMessage(results.Rows[rowIdx][colIdx].(string)), nil
		})
	case DataTypeTimestamp:
		return extractTypedColumn(results, func(rowIdx int) (time.Time, error) {
			return ParseJodaTime(results.Rows[rowIdx][colIdx].(string))
		})
	case DataTypeMap:
		// ref: https://github.com/apache/pinot/pull/13906
		return extractTypedColumn(results, func(rowIdx int) (map[string]interface{}, error) {
			return results.Rows[rowIdx][colIdx].(map[string]interface{}), nil
		})
	default:
		log.Error("Column has unknown data type", "columnIdx", colIdx, "dataType", colDataType)
		return make([]int64, results.RowCount())
	}
}

func ParseJodaTime(ts string) (time.Time, error) {
	return time.Parse("2006-01-02 15:04:05", ts)
}

// ExtractColumnAsDoubles returns the column as a slice of float64.
// Returns an error if the column is not a numeric type.
func ExtractColumnAsDoubles(results *ResultTable, colIdx int) ([]float64, error) {
	colDataType := results.DataSchema.ColumnDataTypes[colIdx]
	switch colDataType {
	case DataTypeInt, DataTypeLong, DataTypeFloat, DataTypeDouble:
		return extractTypedColumn[float64](results, func(rowIdx int) (float64, error) {
			return extractDouble(results.Rows[rowIdx][colIdx])
		}), nil
	}

	switch rawVals := ExtractColumn(results, colIdx).(type) {
	case []*big.Int:
		vals := make([]float64, results.RowCount())
		for i := range rawVals {
			vals[i], _ = rawVals[i].Float64()
		}
		return vals, nil
	default:
		return nil, errors.New("not a numeric column")
	}
}

func extractDouble(v interface{}) (float64, error) {
	if rawVal, ok := v.(string); ok {
		switch rawVal {
		case "-Infinity":
			return math.Inf(-1), nil
		case "Infinity":
			return math.Inf(1), nil
		case "NaN":
			return math.NaN(), nil
		}
	}
	return v.(json.Number).Float64()
}

// ExtractColumnAsStrings returns the column as a slice of strings.
// Non-string types are coerced into strings.
func ExtractColumnAsStrings(results *ResultTable, colIdx int) []string {
	colDataType := results.DataSchema.ColumnDataTypes[colIdx]
	switch colDataType {
	case DataTypeFloat, DataTypeDouble:
		// Parse the floats to standardize the format.
	case DataTypeInt, DataTypeLong:
		return extractTypedColumn[string](results, func(rowIdx int) (string, error) {
			return (results.Rows[rowIdx][colIdx]).(json.Number).String(), nil
		})
	case DataTypeString, DataTypeJson, DataTypeTimestamp, DataTypeBigDecimal:
		return extractTypedColumn[string](results, func(rowIdx int) (string, error) {
			return (results.Rows[rowIdx][colIdx]).(string), nil
		})
	}

	vals := make([]string, results.RowCount())
	switch rawVals := ExtractColumn(results, colIdx).(type) {
	case []float32:
		for i := range rawVals {
			vals[i] = fmt.Sprintf("%v", rawVals[i])
		}
	case []float64:
		for i := range rawVals {
			vals[i] = fmt.Sprintf("%v", rawVals[i])
		}
	case []bool:
		for i := range rawVals {
			vals[i] = fmt.Sprintf("%v", rawVals[i])
		}
	case [][]byte:
		for i := range rawVals {
			vals[i] = string(rawVals[i])
		}
	case []map[string]interface{}:
		for i := range rawVals {
			valJson, _ := json.Marshal(rawVals[i])
			vals[i] = string(valJson)
		}
	}
	return vals
}

// ExtractColumnAsExprs returns the column as a slice of sql expressions representing the column value.
// Strings will be single-quoted. Numbers and booleans are unquoted.
func ExtractColumnAsExprs(results *ResultTable, colIdx int) []string {
	colDataType := results.DataSchema.ColumnDataTypes[colIdx]
	switch colDataType {
	case DataTypeInt, DataTypeLong, DataTypeFloat, DataTypeDouble:
		return extractTypedColumn[string](results, func(rowIdx int) (string, error) {
			if str, ok := (results.Rows[rowIdx][colIdx]).(string); ok {
				return StringLiteralExpr(str), nil
			}
			return (results.Rows[rowIdx][colIdx]).(json.Number).String(), nil
		})
	case DataTypeString, DataTypeJson, DataTypeBytes, DataTypeBigDecimal:
		return extractTypedColumn[string](results, func(rowIdx int) (string, error) {
			return StringLiteralExpr(results.Rows[rowIdx][colIdx].(string)), nil
		})
	}

	exprs := make([]string, results.RowCount())
	switch rawVals := ExtractColumn(results, colIdx).(type) {
	case []time.Time:
		for i := range rawVals {
			exprs[i] = fmt.Sprintf("%d", rawVals[i].UnixMilli())
		}
	case []bool:
		for i := range rawVals {
			exprs[i] = fmt.Sprintf("%v", rawVals[i])
		}
	}
	return exprs
}

// ExtractColumnAsTime returns the column as a slice of time.Time.
// If the column type is LONG, then the value is parsed using the provided format.
// Returns an error if the column type is not LONG or TIMESTAMP.
func ExtractColumnAsTime(results *ResultTable, colIdx int, format DateTimeFormat) ([]time.Time, error) {
	parseLong := func(v int64) time.Time {
		switch format.Unit {
		case TimeUnitDays:
			return time.Unix(86400*v*int64(format.Size), 0).UTC()
		case TimeUnitHours:
			return time.Unix(3600*v*int64(format.Size), 0).UTC()
		case TimeUnitMinutes:
			return time.Unix(60*v*int64(format.Size), 0).UTC()
		case TimeUnitSeconds:
			return time.Unix(v*int64(format.Size), 0).UTC()
		case TimeUnitMilliseconds:
			return time.UnixMilli(v * int64(format.Size)).UTC()
		case TimeUnitMicroseconds:
			return time.UnixMicro(v * int64(format.Size)).UTC()
		default:
			return time.Unix(0, v*int64(format.Size)).UTC()
		}
	}

	switch rawVals := ExtractColumn(results, colIdx).(type) {
	case []int64:
		exprs := make([]time.Time, results.RowCount())
		for i := range rawVals {
			exprs[i] = parseLong(rawVals[i])
		}
		return exprs, nil
	case []time.Time:
		return rawVals, nil
	default:
		return nil, errors.New("not a timestamp column")
	}
}

// DecodeJsonFromColumn decodes each value in the column as type V.
// Returns the first error encountered.
// Returns an error if the column type is not STRING, BYTES, or JSON.
func DecodeJsonFromColumn[V any](results *ResultTable, colIdx int) ([]V, error) {
	colDataType := results.DataSchema.ColumnDataTypes[colIdx]
	switch colDataType {
	case DataTypeString, DataTypeJson, DataTypeBytes:
		break
	default:
		return nil, errors.New("column does not contain json")
	}

	decode := func(src []byte, dest *V, rowIdx int) error {
		decoder := json.NewDecoder(bytes.NewReader(src))
		decoder.UseNumber()
		if err := decoder.Decode(&dest); err != nil {
			return fmt.Errorf("failed to unmarshal json at row %d, column %d: %v", rowIdx, colIdx, err)
		}
		return nil
	}

	vals := make([]V, results.RowCount())
	switch rawVals := ExtractColumn(results, colIdx).(type) {
	case []string:
		for i := range rawVals {
			if err := decode([]byte(rawVals[i]), &vals[i], i); err != nil {
				return nil, err
			}
		}
	case []json.RawMessage:
		for i := range rawVals {
			if err := decode(rawVals[i], &vals[i], i); err != nil {
				return nil, err
			}
		}
	case [][]byte:
		for i := range rawVals {
			if err := decode(rawVals[i], &vals[i], i); err != nil {
				return nil, err
			}
		}
	}
	return vals, nil
}

type nativeColumnType interface {
	int32 | int64 | float32 | float64 | *big.Int | bool | string | []byte | time.Time | json.RawMessage | map[string]interface{}
}

func extractTypedColumn[V nativeColumnType](results *ResultTable, getter func(rowIdx int) (V, error)) []V {
	values := make([]V, results.RowCount())
	hasError := false
	for rowIdx := 0; rowIdx < results.RowCount(); rowIdx++ {
		val, err := getter(rowIdx)
		values[rowIdx] = val

		// Only log the first error.
		if err != nil && !hasError {
			log.WithError(err).Error("Failed to extract column")
			hasError = true
		}
	}
	return values
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
