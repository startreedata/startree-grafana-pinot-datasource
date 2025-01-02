package dataquery

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"math/big"
)

func ExtractTableDataFrame(results *pinotlib.ResultTable, timeColumn string) *data.Frame {
	frame := data.NewFrame("response")

	timeIdx, timeCol := ExtractTimeField(results, timeColumn)
	if timeCol != nil {
		frame.Fields = append(frame.Fields, timeCol)
	}

	for colId := 0; colId < results.ColumnCount(); colId++ {
		if colId == timeIdx {
			continue
		}
		frame.Fields = append(frame.Fields, ExtractColumnAsField(results, colId))
	}
	return frame
}

func ExtractLogsDataFrame(results *pinotlib.ResultTable, timeColumn, logColumn string) (*data.Frame, error) {
	linesIdx, err := pinotlib.GetColumnIdx(results, logColumn)
	if err != nil {
		return nil, fmt.Errorf("could not extract log lines column: %w", err)
	}
	linesCol := pinotlib.ExtractColumnAsStrings(results, linesIdx)

	timeIdx, err := pinotlib.GetColumnIdx(results, timeColumn)
	if err != nil {
		return nil, fmt.Errorf("could not extract time column: %w", err)
	}
	timeCol, err := pinotlib.ExtractColumnAsTime(results, timeIdx, TimeOutputFormat())
	if err != nil {
		return nil, fmt.Errorf("could not extract time column: %w", err)
	}

	dims := make(map[string][]string, results.ColumnCount()-2)
	for colIdx := 0; colIdx < results.ColumnCount(); colIdx++ {
		if colIdx == timeIdx {
			continue
		}
		if colIdx == linesIdx {
			continue
		}
		colName := results.DataSchema.ColumnNames[colIdx]
		dims[colName] = pinotlib.ExtractColumnAsStrings(results, colIdx)
	}

	labelsCol := make([]json.RawMessage, results.RowCount())
	for i := range labelsCol {
		labels := make(map[string]string, len(dims))
		for name, col := range dims {
			labels[name] = col[i]
		}
		labelsCol[i], err = json.Marshal(labels)
		if err != nil {
			return nil, fmt.Errorf("failed to encode labels: %w", err)
		}
	}

	frame := data.NewFrame("response")
	frame.Meta = &data.FrameMeta{
		Custom: map[string]interface{}{"frameType": "LabeledTimeValues"},
	}
	frame.Fields = data.Fields{
		data.NewField("labels", nil, labelsCol),
		data.NewField("Line", nil, linesCol),
		data.NewField("Time", nil, timeCol),
	}
	return frame, nil
}

func ExtractTimeField(results *pinotlib.ResultTable, timeColumn string) (int, *data.Field) {
	timeIdx, err := pinotlib.GetColumnIdx(results, timeColumn)
	if err != nil {
		return -1, nil
	}

	timeCol, err := pinotlib.ExtractColumnAsTime(results, timeIdx, TimeOutputFormat())
	if err != nil {
		return -1, nil
	}

	return timeIdx, data.NewField(timeColumn, nil, timeCol)
}

func ExtractColumnAsField(results *pinotlib.ResultTable, colIdx int) *data.Field {
	colName := results.DataSchema.ColumnNames[colIdx]
	switch col := pinotlib.ExtractColumn(results, colIdx).(type) {
	case [][]byte:
		vals := make([]string, len(col))
		for i := range col {
			vals[i] = hex.EncodeToString(col[i])
		}
		return data.NewField(colName, nil, vals)
	case []map[string]interface{}:
		vals := make([]json.RawMessage, len(col))
		for i := range col {
			vals[i], _ = json.Marshal(col[i])
		}
		return data.NewField(colName, nil, vals)
	case []*big.Int:
		vals := make([]string, len(col))
		for i := range col {
			vals[i] = col[i].String()
		}
		return data.NewField(colName, nil, vals)

	default:
		return data.NewField(colName, nil, col)
	}
}
