package plugin

import (
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/pinot-client-go/pinot"
)

type SqlContext struct {
	RawSql string `json:"rawSql"`
}

type SqlDriver struct {
	queryCtx QueryContext
}

func NewSqlDriver(queryCtx QueryContext) SqlDriver { return SqlDriver{queryCtx} }

func (p SqlDriver) RenderPinotSql() (string, error) {
	rendered, err := ExpandMacros(p.queryCtx, p.queryCtx.SqlContext.RawSql)
	if err != nil {
		return "", err
	}
	return rendered, nil
}

func (p SqlDriver) ExtractResults(results *pinot.ResultTable) (*data.Frame, error) {
	frame := data.NewFrame("response")
	for colId := 0; colId < results.GetColumnCount(); colId++ {
		frame.Fields = append(frame.Fields, ExtractColumnToField(results, colId))
	}
	return frame, nil
}
