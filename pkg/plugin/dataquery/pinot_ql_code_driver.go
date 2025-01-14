package dataquery

import (
	"context"
	"errors"
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
		params.TimeColumnAlias = BuilderTimeColumn
	}
	if params.MetricColumnAlias == "" {
		params.MetricColumnAlias = BuilderMetricColumn
	}
	if params.LogColumnAlias == "" {
		params.LogColumnAlias = BuilderLogColumn
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
	if p.params.Code == "" {
		return NewEmptyDataResponse()
	}

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
		return ExtractTableDataFrame(results, p.params.TimeColumnAlias), nil
	case DisplayTypeLogs:
		return ExtractLogsDataFrame(results, p.params.TimeColumnAlias, p.params.LogColumnAlias)
	default:
		return ExtractTimeSeriesDataFrame(TimeSeriesExtractorParams{
			MetricName:        p.params.MetricColumnAlias,
			Legend:            p.params.Legend,
			TimeColumnAlias:   p.params.TimeColumnAlias,
			MetricColumnAlias: p.params.MetricColumnAlias,
			TimeColumnFormat:  OutputTimeFormat(),
		}, results)
	}
}
