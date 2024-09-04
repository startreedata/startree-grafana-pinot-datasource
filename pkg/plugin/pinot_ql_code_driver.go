package plugin

import (
	"errors"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/pinot-client-go/pinot"
	"time"
)

const DisplayTypeTable = "TABLE"
const DisplayTypeTimeSeries = "TIMESERIES"

type PinotQlCodeDriverParams struct {
	Code              string
	DatabaseName      string
	TableName         string
	TimeColumnAlias   string
	TimeColumnFormat  string
	MetricColumnAlias string
	TimeRange         TimeRange
	IntervalSize      time.Duration
	TableSchema       TableSchema
	DisplayType       string
	Legend            string
}

type PinotQlCodeDriver struct {
	PinotQlCodeDriverParams
	MacroEngine
}

func NewPinotQlCodeDriver(params PinotQlCodeDriverParams) (*PinotQlCodeDriver, error) {
	if params.TableName == "" {
		return nil, errors.New("TableName is required")
	} else if params.IntervalSize == 0 {
		return nil, errors.New("IntervalSize is required")
	} else if params.Code == "" {
		return nil, errors.New("Code is required")
	}

	if params.TimeColumnAlias == "" {
		params.TimeColumnAlias = DefaultTimeColumnAlias
	}
	if params.TimeColumnFormat == "" {
		params.TimeColumnFormat = TimeGroupExprOutputFormat
	}
	if params.MetricColumnAlias == "" {
		params.MetricColumnAlias = DefaultMetricColumnAlias
	}

	return &PinotQlCodeDriver{
		PinotQlCodeDriverParams: params,
		MacroEngine: MacroEngine{
			TableName:    params.TableName,
			TableSchema:  params.TableSchema,
			TimeRange:    params.TimeRange,
			IntervalSize: params.IntervalSize,
			TimeAlias:    params.TimeColumnAlias,
			MetricAlias:  params.MetricColumnAlias,
		}}, nil
}

func (p *PinotQlCodeDriver) RenderPinotSql() (string, error) {
	rendered, err := p.MacroEngine.ExpandMacros(p.Code)
	if err != nil {
		return "", err
	}
	return rendered, nil
}

func (p *PinotQlCodeDriver) ExtractResults(results *pinot.ResultTable) (*data.Frame, error) {
	switch p.DisplayType {
	case DisplayTypeTable:
		return p.ExtractTableResults(results)
	case DisplayTypeTimeSeries:
		return p.ExtractTimeSeriesResults(results)
	default:
		return p.ExtractTimeSeriesResults(results)
	}
}

func (p *PinotQlCodeDriver) ExtractTimeSeriesResults(results *pinot.ResultTable) (*data.Frame, error) {
	return ExtractTimeSeriesDataFrame(TimeSeriesExtractorParams{
		MetricName:        p.MetricColumnAlias,
		Legend:            p.Legend,
		TimeColumnAlias:   p.TimeColumnAlias,
		TimeColumnFormat:  p.TimeColumnFormat,
		MetricColumnAlias: p.MetricColumnAlias,
	}, results)
}

func (p *PinotQlCodeDriver) ExtractTableResults(results *pinot.ResultTable) (*data.Frame, error) {
	frame := data.NewFrame("response")

	timeIdx, timeCol := p.extractTableTime(results)
	if timeCol != nil {
		frame.Fields = append(frame.Fields, timeCol)
	}

	for colId := 0; colId < results.GetColumnCount(); colId++ {
		if colId == timeIdx {
			continue
		}
		frame.Fields = append(frame.Fields, ExtractColumnToField(results, colId))
	}
	return frame, nil
}

func (p *PinotQlCodeDriver) extractTableTime(results *pinot.ResultTable) (int, *data.Field) {
	timeIdx, err := GetColumnIdx(results, p.TimeColumnAlias)
	if err != nil {
		return -1, nil
	}

	timeCol, err := ExtractTimeColumn(results, timeIdx, p.TimeColumnFormat)
	if err != nil {
		return -1, nil
	}

	return timeIdx, data.NewField(p.TimeColumnAlias, nil, timeCol)
}
