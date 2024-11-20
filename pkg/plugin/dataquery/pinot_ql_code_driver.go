package dataquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"time"
)

const (
	DisplayTypeTable      = "TABLE"
	DisplayTypeTimeSeries = "TIMESERIES"
	DisplayTypeLogs       = "LOGS"
)

type PinotQlCodeDriverParams struct {
	Ctx               context.Context
	PinotClient       *pinotlib.PinotClient
	Code              string
	TableName         string
	TimeColumnAlias   string
	MetricColumnAlias string
	LogColumnAlias    string
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

	if params.Ctx == nil {
		params.Ctx = context.Background()
	}
	if params.TimeColumnAlias == "" {
		params.TimeColumnAlias = DefaultTimeColumnAlias
	}
	if params.MetricColumnAlias == "" {
		params.MetricColumnAlias = DefaultMetricColumnAlias
	}
	if params.LogColumnAlias == "" {
		params.LogColumnAlias = DefaultLogColumnAlias
	}

	tableConfigs, err := params.PinotClient.ListTableConfigs(params.Ctx, params.TableName)
	if err != nil {
		log.WithError(err).FromContext(params.Ctx).Error("failed to fetch table config")
	}

	return &PinotQlCodeDriver{
		params: params,
		macroEngine: MacroEngine{
			Ctx:          params.Ctx,
			TableName:    params.TableName,
			TableSchema:  params.TableSchema,
			TableConfigs: tableConfigs,
			TimeRange:    params.TimeRange,
			IntervalSize: params.IntervalSize,
			TimeAlias:    params.TimeColumnAlias,
			MetricAlias:  params.MetricColumnAlias,
		},
	}, nil
}

func (p *PinotQlCodeDriver) Execute(ctx context.Context) backend.DataResponse {
	sql, err := p.RenderPinotSql()
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, p.params.PinotClient, pinotlib.NewSqlQuery(sql))
	if !ok {
		return backendResp
	}

	frame, err := p.ExtractResults(results)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	return NewSqlQueryDataResponse(frame, exceptions)
}

func (p *PinotQlCodeDriver) RenderPinotSql() (string, error) {
	rendered, err := p.macroEngine.ExpandMacros(p.params.Code)
	if err != nil {
		return "", err
	}
	return rendered, nil
}

func (p *PinotQlCodeDriver) ExtractResults(results *pinotlib.ResultTable) (*data.Frame, error) {
	switch p.params.DisplayType {
	case DisplayTypeTable:
		return p.ExtractTableResults(results)
	case DisplayTypeTimeSeries:
		return p.ExtractTimeSeriesResults(results)
	case DisplayTypeLogs:
		return p.ExtractLogResults(results)
	default:
		return p.ExtractTimeSeriesResults(results)
	}
}

func (p *PinotQlCodeDriver) ExtractTimeSeriesResults(results *pinotlib.ResultTable) (*data.Frame, error) {
	return ExtractTimeSeriesDataFrame(TimeSeriesExtractorParams{
		MetricName:        p.params.MetricColumnAlias,
		Legend:            p.params.Legend,
		TimeColumnAlias:   p.params.TimeColumnAlias,
		MetricColumnAlias: p.params.MetricColumnAlias,
		TimeColumnFormat:  pinotlib.DateTimeFormatMillisecondsEpoch(),
	}, results)
}

func (p *PinotQlCodeDriver) ExtractTableResults(results *pinotlib.ResultTable) (*data.Frame, error) {
	frame := data.NewFrame("response")

	timeIdx, timeCol := p.extractTableTime(results)
	if timeCol != nil {
		frame.Fields = append(frame.Fields, timeCol)
	}

	for colId := 0; colId < results.ColumnCount(); colId++ {
		if colId == timeIdx {
			continue
		}
		frame.Fields = append(frame.Fields, pinotlib.ExtractColumnToField(results, colId))
	}
	return frame, nil
}

func (p *PinotQlCodeDriver) ExtractLogResults(results *pinotlib.ResultTable) (*data.Frame, error) {

	linesIdx, err := pinotlib.GetColumnIdx(results, p.params.LogColumnAlias)
	if err != nil {
		return nil, fmt.Errorf("could not extract log lines column: %w", err)
	}
	linesCol := pinotlib.ExtractStringColumn(results, linesIdx)

	timeIdx, err := pinotlib.GetColumnIdx(results, p.params.TimeColumnAlias)
	if err != nil {
		return nil, fmt.Errorf("could not extract time column: %w", err)
	}
	timeCol, err := pinotlib.ExtractTimeColumn(results, timeIdx, pinotlib.DateTimeFormatMillisecondsEpoch())
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
		dims[colName] = pinotlib.ExtractStringColumn(results, colIdx)
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

func (p *PinotQlCodeDriver) extractTableTime(results *pinotlib.ResultTable) (int, *data.Field) {
	timeIdx, err := pinotlib.GetColumnIdx(results, p.params.TimeColumnAlias)
	if err != nil {
		return -1, nil
	}

	timeCol, err := pinotlib.ExtractTimeColumn(results, timeIdx, pinotlib.DateTimeFormatMillisecondsEpoch())
	if err != nil {
		return -1, nil
	}

	return timeIdx, data.NewField(p.params.TimeColumnAlias, nil, timeCol)
}
