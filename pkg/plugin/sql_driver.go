package plugin

import (
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/pinot-client-go/pinot"
)

type SqlContext struct {
	RawSql string `json:"rawSql"`
}

type SqlDriver struct {
	client PinotClient
}

func NewSqlDriver() SqlDriver { return SqlDriver{} }

func (p SqlDriver) RenderPinotSql(queryCtx QueryContext) (string, error) {
	newQueryCtx, err := ExpandMacros(queryCtx)
	if err != nil {
		return "", err
	}
	return newQueryCtx.SqlContext.RawSql, nil
}

func (p SqlDriver) ExtractResults(results *pinot.ResultTable) (*data.Frame, error) {
	frame := data.NewFrame("response")
	for colId := 0; colId < results.GetColumnCount(); colId++ {
		frame.Fields = append(frame.Fields, extractMetricColumn(results, colId))
	}
	return frame, nil
}

func getColumnIdx(resultTable *pinot.ResultTable, colName string) (int, bool) {
	columnNames := resultTable.DataSchema.ColumnNames
	for idx := 0; idx < len(columnNames); idx++ {
		if columnNames[idx] == colName {
			return idx, true
		}
	}
	return -1, false
}

func extractMetricColumn(results *pinot.ResultTable, colIdx int) *data.Field {
	colName := results.DataSchema.ColumnNames[colIdx]
	colDataType := results.DataSchema.ColumnDataTypes[colIdx]

	var values interface{}
	switch colDataType {
	case "INT", "LONG":
		values = extractTypedColumn[int64](results, colIdx, results.GetLong)
	case "FLOAT", "DOUBLE":
		values = extractTypedColumn[float64](results, colIdx, results.GetDouble)
	case "STRING":
		values = extractTypedColumn[string](results, colIdx, results.GetString)
	}
	return data.NewField(colName, nil, values)
}

func extractTypedColumn[V int64 | float64 | string](results *pinot.ResultTable, colIdx int, getter func(rowIdx, colIdx int) V) []V {
	values := make([]V, results.GetRowCount())
	for rowIdx := 0; rowIdx < results.GetRowCount(); rowIdx++ {
		values[rowIdx] = getter(rowIdx, colIdx)
	}
	return values
}
