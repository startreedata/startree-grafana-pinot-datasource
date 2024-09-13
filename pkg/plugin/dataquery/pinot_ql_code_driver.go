package dataquery

import (
	"context"
	"errors"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/pinot-client-go/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"time"
)

const (
	DisplayTypeTable      = "TABLE"
	DisplayTypeTimeSeries = "TIMESERIES"
)

type PinotQlCodeDriverParams struct {
	PinotClient       *pinotlib.PinotClient
	Code              string
	DatabaseName      string
	TableName         string
	TimeColumnAlias   string
	TimeColumnFormat  string
	MetricColumnAlias string
	TimeRange         TimeRange
	IntervalSize      time.Duration
	TableSchema       pinotlib.TableSchema
	DisplayType       string
	Legend            string
}

type PinotQlCodeDriver struct {
	params      PinotQlCodeDriverParams
	macroEngine MacroEngine
}

func NewPinotQlCodeDriver(params PinotQlCodeDriverParams) (*PinotQlCodeDriver, error) {
	if params.TableName == "" {
		return nil, errors.New("field `TableName` is required")
	} else if params.IntervalSize == 0 {
		return nil, errors.New("field `IntervalSize` is required")
	} else if params.Code == "" {
		return nil, errors.New("field `Code` is required")
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

	macroEngine := MacroEngine{
		TableName:    params.TableName,
		TableSchema:  params.TableSchema,
		TimeRange:    params.TimeRange,
		IntervalSize: params.IntervalSize,
		TimeAlias:    params.TimeColumnAlias,
		MetricAlias:  params.MetricColumnAlias,
	}

	return &PinotQlCodeDriver{
		params:      params,
		macroEngine: macroEngine,
	}, nil
}

func (p *PinotQlCodeDriver) Execute(ctx context.Context) backend.DataResponse {
	sql, err := p.RenderPinotSql()
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}

	resp, err := p.params.PinotClient.ExecuteSQL(ctx, p.params.TableName, sql)
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}

	frame, err := p.ExtractResults(resp.ResultTable)
	if err != nil {
		return NewDataInternalErrorResponse(err)
	}

	return NewDataResponse(frame)
}

func (p *PinotQlCodeDriver) RenderPinotSql() (string, error) {
	rendered, err := p.macroEngine.ExpandMacros(p.params.Code)
	if err != nil {
		return "", err
	}
	return rendered, nil
}

func (p *PinotQlCodeDriver) ExtractResults(results *pinot.ResultTable) (*data.Frame, error) {
	switch p.params.DisplayType {
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
		MetricName:        p.params.MetricColumnAlias,
		Legend:            p.params.Legend,
		TimeColumnAlias:   p.params.TimeColumnAlias,
		TimeColumnFormat:  p.params.TimeColumnFormat,
		MetricColumnAlias: p.params.MetricColumnAlias,
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
		frame.Fields = append(frame.Fields, pinotlib.ExtractColumnToField(results, colId))
	}
	return frame, nil
}

func (p *PinotQlCodeDriver) extractTableTime(results *pinot.ResultTable) (int, *data.Field) {
	timeIdx, err := pinotlib.GetColumnIdx(results, p.params.TimeColumnAlias)
	if err != nil {
		return -1, nil
	}

	timeCol, err := pinotlib.ExtractTimeColumn(results, timeIdx, p.params.TimeColumnFormat)
	if err != nil {
		return -1, nil
	}

	return timeIdx, data.NewField(p.params.TimeColumnAlias, nil, timeCol)
}
