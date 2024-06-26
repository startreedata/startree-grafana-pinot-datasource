package plugin

import (
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/pinot-client-go/pinot"
)

type SqlContext struct {
	RawSql string `json:"rawSql"`
}

type SqlTableDriver struct {
	RawQuery    string
	MacroEngine MacroEngine
}

// TODO: Redo this constructor.
func NewPinotQlCodeDriver(query PinotDataQuery, tableSchema TableSchema, timeRange TimeRange) SqlTableDriver {
	return SqlTableDriver{
		RawQuery: query.RawSql,
		MacroEngine: MacroEngine{
			TableName:    query.TableName,
			TableSchema:  tableSchema,
			TimeRange:    timeRange,
			IntervalSize: query.IntervalSize,
		}}
}

func (p SqlTableDriver) RenderPinotSql() (string, error) {
	rendered, err := p.MacroEngine.ExpandMacros(p.RawQuery)
	if err != nil {
		return "", err
	}
	return rendered, nil
}

func (p SqlTableDriver) ExtractResults(results *pinot.ResultTable) (*data.Frame, error) {
	frame := data.NewFrame("response")
	for colId := 0; colId < results.GetColumnCount(); colId++ {
		frame.Fields = append(frame.Fields, ExtractColumnToField(results, colId))
	}
	return frame, nil
}
