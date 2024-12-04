package dataquery

import (
	"context"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/templates"
	"strings"
	"time"
)

const (
	DefaultTimeColumnAlias   = "time"
	DefaultMetricColumnAlias = "metric"
	DefaultLogColumnAlias    = "message"

	AggregationFunctionCount = "COUNT"
	AggregationFunctionNone  = "NONE"

	DefaultLimit = 100_000
)

type PinotQlBuilderDriver struct {
	params            PinotQlBuilderParams
	TimeColumnAlias   string
	MetricColumnAlias string
	TimeGroup         pinotlib.DateTimeConversion
	TableConfigs      pinotlib.ListTableConfigsResponse
}

type PinotQlBuilderParams struct {
	Ctx                 context.Context
	PinotClient         *pinotlib.PinotClient
	TableSchema         pinotlib.TableSchema
	TimeRange           TimeRange
	IntervalSize        time.Duration
	TableName           string
	TimeColumn          string
	MetricColumn        string
	GroupByColumns      []string
	AggregationFunction string
	DimensionFilters    []DimensionFilter
	Limit               int64
	Granularity         string
	MaxDataPoints       int64
	OrderByClauses      []OrderByClause
	QueryOptions        []QueryOption
	Legend              string
}

func NewPinotQlBuilderDriver(params PinotQlBuilderParams) (*PinotQlBuilderDriver, error) {
	if params.TableName == "" {
		return nil, errors.New("TableName is required")
	} else if params.TimeColumn == "" {
		return nil, errors.New("TimeColumn is required")
	} else if params.MetricColumn == "" && params.AggregationFunction != AggregationFunctionCount {
		return nil, errors.New("MetricColumn is required")
	} else if params.AggregationFunction == "" {
		return nil, errors.New("AggregationFunction is required")
	}

	if params.Ctx == nil {
		params.Ctx = context.Background()
	}

	timeColumnFormat, err := pinotlib.GetTimeColumnFormat(params.TableSchema, params.TimeColumn)
	if err != nil {
		return nil, err
	}

	tableConfigs, err := params.PinotClient.ListTableConfigs(params.Ctx, params.TableName)
	if err != nil {
		log.WithError(err).FromContext(params.Ctx).Error("failed to fetch table config")
	}

	derivedGranularities := pinotlib.DerivedGranularitiesFor(tableConfigs, params.TimeColumn)
	granularity := ResolveGranularity(params.Ctx, params.Granularity, timeColumnFormat, params.IntervalSize, derivedGranularities)

	return &PinotQlBuilderDriver{
		params:            params,
		TimeColumnAlias:   DefaultTimeColumnAlias,
		MetricColumnAlias: DefaultMetricColumnAlias,
		TableConfigs:      tableConfigs,
		TimeGroup: pinotlib.DateTimeConversion{
			TimeColumn:   params.TimeColumn,
			InputFormat:  timeColumnFormat,
			OutputFormat: pinotlib.DateTimeFormatMillisecondsEpoch(),
			Granularity:  granularity,
		},
	}, nil
}

func (p *PinotQlBuilderDriver) Execute(ctx context.Context) backend.DataResponse {
	sql, err := p.RenderPinotSql(true)
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

func (p *PinotQlBuilderDriver) RenderPinotSql(expandMacros bool) (string, error) {
	if p.params.AggregationFunction == AggregationFunctionNone {
		return templates.RenderSingleMetricSql(templates.SingleMetricSqlParams{
			TableNameExpr:         p.tableNameExpr(expandMacros),
			TimeColumn:            p.params.TimeColumn,
			TimeColumnAliasExpr:   p.timeColumnAliasExpr(expandMacros),
			MetricColumn:          p.params.MetricColumn,
			MetricColumnAliasExpr: p.metricColumnAliasExpr(expandMacros),
			TimeFilterExpr:        p.timeFilterExpr(expandMacros),
			DimensionFilterExprs:  FilterExprsFrom(p.params.DimensionFilters),
			Limit:                 p.resolveLimit(),
			QueryOptionsExpr:      p.queryOptionsExpr(),
		})
	} else {
		return templates.RenderTimeSeriesSql(templates.TimeSeriesSqlParams{
			TableNameExpr:         p.tableNameExpr(expandMacros),
			TimeGroupExpr:         p.timeGroupExpr(expandMacros),
			TimeColumnAliasExpr:   p.timeColumnAliasExpr(expandMacros),
			AggregationFunction:   p.params.AggregationFunction,
			MetricColumn:          p.resolveMetricColumn(),
			MetricColumnAliasExpr: p.metricColumnAliasExpr(expandMacros),
			GroupByColumns:        p.params.GroupByColumns,
			TimeFilterExpr:        p.timeFilterExpr(expandMacros),
			DimensionFilterExprs:  FilterExprsFrom(p.params.DimensionFilters),
			Limit:                 p.resolveLimit(),
			OrderByExprs:          p.orderByExprs(),
			QueryOptionsExpr:      p.queryOptionsExpr(),
		})
	}
}

func (p *PinotQlBuilderDriver) ExtractResults(results *pinotlib.ResultTable) (*data.Frame, error) {
	return ExtractTimeSeriesDataFrame(TimeSeriesExtractorParams{
		MetricName:        p.resolveMetricName(),
		Legend:            p.params.Legend,
		TimeColumnAlias:   p.TimeColumnAlias,
		MetricColumnAlias: p.MetricColumnAlias,
		TimeColumnFormat:  p.resolveTimeColumnFormat(),
	}, results)
}

func (p *PinotQlBuilderDriver) tableNameExpr(expandMacros bool) string {
	if expandMacros {
		return pinotlib.ObjectExpr(p.params.TableName)
	} else {
		return MacroExprFor(MacroTable)
	}
}

func (p *PinotQlBuilderDriver) timeColumnAliasExpr(expandMacros bool) string {
	if expandMacros {
		return pinotlib.ObjectExpr(p.TimeColumnAlias)
	} else {
		return MacroExprFor(MacroTimeAlias)
	}
}

func (p *PinotQlBuilderDriver) metricColumnAliasExpr(expandMacros bool) string {
	if expandMacros {
		return pinotlib.ObjectExpr(p.MetricColumnAlias)
	} else {
		return MacroExprFor(MacroMetricAlias)
	}
}

func (p *PinotQlBuilderDriver) timeFilterExpr(expandMacros bool) string {
	if expandMacros {
		return pinotlib.TimeFilterBucketAlignedExpr(pinotlib.TimeFilter{
			Column: p.params.TimeColumn,
			Format: p.TimeGroup.InputFormat,
			From:   p.params.TimeRange.From,
			To:     p.params.TimeRange.To,
		}, p.TimeGroup.Granularity.Duration())
	} else {
		return MacroExprFor(MacroTimeFilter, pinotlib.ObjectExpr(p.params.TimeColumn), pinotlib.GranularityExpr(p.TimeGroup.Granularity))
	}
}

func (p *PinotQlBuilderDriver) timeGroupExpr(expandMacros bool) string {
	if expandMacros {
		return pinotlib.TimeGroupExpr(p.TableConfigs, p.TimeGroup)
	} else {
		return MacroExprFor(MacroTimeGroup, pinotlib.ObjectExpr(p.params.TimeColumn), pinotlib.GranularityExpr(p.TimeGroup.Granularity))
	}
}

func (p *PinotQlBuilderDriver) resolveTimeColumnFormat() pinotlib.DateTimeFormat {
	if p.params.AggregationFunction == AggregationFunctionNone {
		return p.TimeGroup.InputFormat
	} else {
		return p.TimeGroup.OutputFormat
	}
}

func (p *PinotQlBuilderDriver) resolveMetricName() string {
	if p.params.AggregationFunction == AggregationFunctionCount {
		return "count"
	} else {
		return p.params.MetricColumn
	}
}

func (p *PinotQlBuilderDriver) resolveLimit() int64 {
	switch true {
	case p.params.Limit >= 1:
		// Use provided limit if present
		return p.params.Limit
	case p.params.AggregationFunction != AggregationFunctionNone && len(p.params.GroupByColumns) > 0:
		// Use default limit for group by queries.
		// TODO: Resolve more accurate limit in this case.
		return DefaultLimit
	case p.params.MaxDataPoints > 0:
		// Queries with extra dimensions can directly use max data points.
		return p.params.MaxDataPoints
	default:
		return DefaultLimit
	}
}

func (p *PinotQlBuilderDriver) resolveMetricColumn() string {
	if p.params.AggregationFunction == AggregationFunctionCount {
		return "*"
	} else {
		return p.params.MetricColumn
	}
}

func (p *PinotQlBuilderDriver) orderByExprs() []string {
	orderByExprs := make([]string, 0, len(p.params.OrderByClauses))
	for _, o := range p.params.OrderByClauses {
		if o.ColumnName == "" {
			continue
		}

		var direction string
		if strings.ToUpper(o.Direction) == "DESC" {
			direction = "DESC"
		} else {
			direction = "ASC"
		}

		orderByExprs = append(orderByExprs, fmt.Sprintf(`"%s" %s`, o.ColumnName, direction))
	}
	return orderByExprs[:]
}

func (p *PinotQlBuilderDriver) queryOptionsExpr() string {
	exprs := make([]string, 0, len(p.params.QueryOptions))
	for _, o := range p.params.QueryOptions {
		if o.Name != "" && o.Value != "" {
			exprs = append(exprs, fmt.Sprintf(`SET %s=%s;`, o.Name, o.Value))
		}
	}
	return strings.Join(exprs, "\n")
}
