package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"strconv"
	"time"
)

var queryCounter = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "grafana_plugin",
		Name:      "pinot_data_queries_total",
		Help:      "Total number of queries to the Pinot data source.",
	},
	[]string{"query_type", "status"},
)

var queryDuration = promauto.NewSummaryVec(
	prometheus.SummaryOpts{
		Namespace:  "grafana_plugin",
		Name:       "pinot_data_query_duration_seconds",
		Help:       "Duration of queries to the Pinot data source.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	},
	[]string{"query_type", "status"},
)

const SeriesLimitDisabled int = -1

type ExecutableQuery interface {
	Execute(client *pinotlib.PinotClient, ctx context.Context) backend.DataResponse
}

func ExecuteQuery(client *pinotlib.PinotClient, ctx context.Context, backendQuery backend.DataQuery) backend.DataResponse {
	startTime := time.Now()

	var query DataQuery
	var resp backend.DataResponse
	if err := query.ReadFrom(backendQuery); err != nil {
		resp = backend.ErrDataResponse(backend.StatusBadRequest, err.Error())
	} else {
		resp = ExecutableQueryFrom(query).Execute(client, ctx)
	}

	labels := prometheus.Labels{
		"query_type": query.QueryType.String(),
		"status":     strconv.FormatInt(int64(resp.Status), 10),
	}
	queryCounter.With(labels).Inc()
	queryDuration.With(labels).Observe(time.Since(startTime).Seconds())
	return resp
}

func ExecutableQueryFrom(query DataQuery) ExecutableQuery {
	var seriesLimit int
	if query.SeriesLimit == nil {
		seriesLimit = SeriesLimitDisabled
	} else {
		seriesLimit = *query.SeriesLimit
	}

	switch {
	case query.Hide:
		return new(NoOpQuery)

	case query.QueryType == QueryTypePromQl:
		return PromQlQuery{
			TableName:    query.TableName,
			PromQlCode:   query.PromQlCode,
			TimeRange:    query.TimeRange,
			IntervalSize: query.IntervalSize,
			Legend:       query.Legend,
			SeriesLimit:  seriesLimit,
		}

	case query.QueryType == QueryTypePinotVariableQuery:
		return VariableQuery{
			TableName:    query.TableName,
			VariableType: query.VariableQuery.VariableType,
			ColumnName:   query.VariableQuery.ColumnName,
			PinotQlCode:  query.VariableQuery.PinotQlCode,
			ColumnType:   query.VariableQuery.ColumnType,
		}

	case query.QueryType == QueryTypePinotQl && query.EditorMode == EditorModeCode:
		return PinotQlCodeQuery{
			Code:              query.PinotQlCode,
			TableName:         query.TableName,
			TimeColumnAlias:   query.TimeColumnAlias,
			MetricColumnAlias: query.MetricColumnAlias,
			LogColumnAlias:    query.LogColumnAlias,
			TimeRange:         query.TimeRange,
			IntervalSize:      query.IntervalSize,
			DisplayType:       query.DisplayType,
			Legend:            query.Legend,
			SeriesLimit:       seriesLimit,
		}

	case query.QueryType == QueryTypePinotQl && query.EditorMode == EditorModeBuilder && query.DisplayType == DisplayTypeLogs:
		return LogsBuilderQuery{
			TimeRange:        query.TimeRange,
			TableName:        query.TableName,
			TimeColumn:       query.TimeColumn,
			LogColumn:        query.LogColumn,
			LogColumnAlias:   query.LogColumnAlias,
			MetadataColumns:  query.MetadataColumns,
			JsonExtractors:   query.JsonExtractors,
			RegexpExtractors: query.RegexpExtractors,
			DimensionFilters: query.DimensionFilters,
			QueryOptions:     query.QueryOptions,
			Limit:            query.Limit,
		}

	case query.QueryType == QueryTypePinotQl && query.EditorMode == EditorModeBuilder:
		var metricColumn ComplexField
		if query.MetricColumnV2.Name != "" {
			metricColumn = query.MetricColumnV2
		} else {
			metricColumn = ComplexField{Name: query.MetricColumn}
		}

		groupByColumns := make([]ComplexField, 0, len(query.GroupByColumns)+len(query.GroupByColumnsV2))
		for _, col := range query.GroupByColumns {
			groupByColumns = append(groupByColumns, ComplexField{Name: col})
		}
		groupByColumns = append(groupByColumns, query.GroupByColumnsV2...)

		return TimeSeriesBuilderQuery{
			TimeRange:           query.TimeRange,
			IntervalSize:        query.IntervalSize,
			TableName:           query.TableName,
			TimeColumn:          query.TimeColumn,
			MetricColumn:        metricColumn,
			GroupByColumns:      groupByColumns,
			AggregationFunction: query.AggregationFunction,
			DimensionFilters:    query.DimensionFilters,
			Limit:               query.Limit,
			Granularity:         query.Granularity,
			OrderByClauses:      query.OrderByClauses,
			QueryOptions:        query.QueryOptions,
			Legend:              query.Legend,
			SeriesLimit:         seriesLimit,
		}

	default:
		return new(NoOpQuery)
	}
}

var _ ExecutableQuery = NoOpQuery{}

type NoOpQuery struct{}

func (d NoOpQuery) Execute(*pinotlib.PinotClient, context.Context) backend.DataResponse {
	return NewEmptyDataResponse()
}

func newSqlQueryWithOptions(sql string, options []QueryOption) pinotlib.SqlQuery {
	query := pinotlib.NewSqlQuery(sql)
	for _, o := range options {
		if o.Name == "" || o.Value == "" {
			continue
		}
		query.QueryOptions = append(query.QueryOptions, pinotlib.QueryOption{Name: o.Name, Value: o.Value})
	}
	return query
}

func doSqlQuery(ctx context.Context, pinotClient *pinotlib.PinotClient, query pinotlib.SqlQuery) (*pinotlib.ResultTable, []pinotlib.BrokerException, bool, backend.DataResponse) {
	resp, err := pinotClient.ExecuteSqlQuery(ctx, query)
	if err != nil {
		return nil, nil, false, NewPluginErrorResponse(err)
	} else if resp.HasData() {
		return resp.ResultTable, resp.Exceptions, true, backend.DataResponse{}
	} else if resp.HasExceptions() {
		return nil, resp.Exceptions, false, NewPinotExceptionsDataResponse(resp.Exceptions)
	} else {
		return nil, nil, false, NewEmptyDataResponse()
	}
}
