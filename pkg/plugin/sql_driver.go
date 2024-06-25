package plugin

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend"
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
func NewSqlTableDriver(query PinotDataQuery, tableSchema TableSchema, timeRange backend.TimeRange) SqlTableDriver {
	return SqlTableDriver{
		RawQuery: query.RawSql,
		MacroEngine: MacroEngine{
			TableName:   query.TableName,
			TableSchema: tableSchema,
			TimeRange: TimeRange{
				To:   timeRange.To,
				From: timeRange.From,
			},
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
