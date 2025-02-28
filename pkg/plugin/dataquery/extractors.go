package dataquery

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"math/big"
)

func ExtractTableDataFrame(results *pinot.ResultTable, timeColumn string) (*data.Frame, error) {
	frame := data.NewFrame("response")

	timeIdx, timeCol := ExtractTimeField(results, timeColumn)
	if timeCol != nil {
		frame.Fields = append(frame.Fields, timeCol)
	}

	for colId := 0; colId < results.ColumnCount(); colId++ {
		if colId == timeIdx {
			continue
		}
		field, err := ExtractColumnAsField(results, colId)
		if err != nil {
			return nil, err
		}
		frame.Fields = append(frame.Fields, field)
	}
	return frame, nil
}

func ExtractLogsDataFrame(results *pinot.ResultTable, timeColumn, logColumn string) (*data.Frame, error) {
	linesIdx, err := pinot.GetColumnIdx(results, logColumn)
	if err != nil {
		return nil, fmt.Errorf("could not extract log lines column: %w", err)
	}
	linesCol, err := pinot.ExtractColumnAsStrings(results, linesIdx)
	if err != nil {
		return nil, fmt.Errorf("could not extract log lines column: %w", err)
	}

	timeIdx, err := pinot.GetColumnIdx(results, timeColumn)
	if err != nil {
		return nil, fmt.Errorf("could not extract time column: %w", err)
	}
	timeCol, err := pinot.ExtractColumnAsTime(results, timeIdx, OutputTimeFormat())
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
		dimCol, err := pinot.ExtractColumnAsStrings(results, colIdx)
		if err != nil {
			return nil, fmt.Errorf("could not extract dimension column %s: %w", colName, err)
		}
		dims[colName] = dimCol
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

func ExtractTimeField(results *pinot.ResultTable, timeColumn string) (int, *data.Field) {
	timeIdx, err := pinot.GetColumnIdx(results, timeColumn)
	if err != nil {
		return -1, nil
	}

	timeCol, err := pinot.ExtractColumnAsTime(results, timeIdx, OutputTimeFormat())
	if err != nil {
		return -1, nil
	}

	return timeIdx, data.NewField(timeColumn, nil, timeCol)
}

func ExtractColumnAsField(results *pinot.ResultTable, colIdx int) (*data.Field, error) {
	colName := results.DataSchema.ColumnNames[colIdx]
	col, err := pinot.ExtractColumn(results, colIdx)
	if err != nil {
		return nil, err
	}
	switch rawValues := col.(type) {
	case [][]byte:
		vals := make([]string, len(rawValues))
		for i := range rawValues {
			vals[i] = hex.EncodeToString(rawValues[i])
		}
		return data.NewField(colName, nil, vals), nil
	case []map[string]interface{}:
		vals := make([]json.RawMessage, len(rawValues))
		for i := range rawValues {
			vals[i], _ = json.Marshal(rawValues[i])
		}
		return data.NewField(colName, nil, vals), nil
	case []*big.Int:
		vals := make([]string, len(rawValues))
		for i := range rawValues {
			vals[i] = rawValues[i].String()
		}
		return data.NewField(colName, nil, vals), nil

	default:
		return data.NewField(colName, nil, rawValues), nil
	}
}
