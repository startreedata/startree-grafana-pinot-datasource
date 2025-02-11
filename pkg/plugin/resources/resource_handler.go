package resources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/auth"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/collections"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/dataquery"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"net/http"
	"sort"
	"strconv"
	"time"
)

var requestCounter = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "grafana_plugin",
		Name:      "pinot_resource_requests_total",
		Help:      "Total number of queries to the Pinot data source.",
	},
	[]string{"endpoint", "status"},
)

var requestDuration = promauto.NewSummaryVec(
	prometheus.SummaryOpts{
		Namespace:  "grafana_plugin",
		Name:       "pinot_resource_request_duration_seconds",
		Help:       "Duration of queries to the Pinot data source.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	},
	[]string{"endpoint", "status"},
)

func NewResourceHandler(client *pinotlib.PinotClient) http.Handler {
	router := mux.NewRouter()
	router.HandleFunc("/databases", adaptHandler(client, ListDatabases))
	router.HandleFunc("/isPromQlSupported", adaptHandler(client, IsPromQlSupported))
	router.HandleFunc("/preview/sql/builder", adaptHandlerWithBody(client, PreviewSqlBuilder))
	router.HandleFunc("/preview/logs/sql", adaptHandlerWithBody(client, PreviewLogsSql))
	router.HandleFunc("/preview/sql/code", adaptHandlerWithBody(client, PreviewSqlCode))
	router.HandleFunc("/preview/sql/distinctValues", adaptHandlerWithBody(client, PreviewSqlDistinctValues))
	router.HandleFunc("/query/distinctValues", adaptHandlerWithBody(client, QueryDistinctValues))
	router.HandleFunc("/tables", adaptHandler(client, ListTables))
	router.HandleFunc("/tables/{table}/schema", adaptHandler(client, GetTableSchema))
	router.HandleFunc("/timeseries/tables", adaptHandler(client, ListTimeSeriesTables))
	router.HandleFunc("/timeseries/metrics", adaptHandlerWithBody(client, ListTimeSeriesMetrics))
	router.HandleFunc("/timeseries/labels", adaptHandlerWithBody(client, ListTimeSeriesLabels))
	router.HandleFunc("/timeseries/labelValues", adaptHandlerWithBody(client, ListTimeSeriesLabelValues))
	router.HandleFunc("/granularities", adaptHandlerWithBody(client, ListSuggestedGranularities))
	router.HandleFunc("/columns", adaptHandlerWithBody(client, ListColumns))
	return router
}

type Response[T any] struct {
	Code   int    `json:"code"`
	Error  string `json:"error,omitempty"`
	Result T      `json:"result,omitempty"`
}

func ListDatabases(client *pinotlib.PinotClient, r *http.Request) *Response[[]string] {
	databases, err := client.ListDatabases(r.Context())

	if pinotlib.IsStatusForbiddenError(err) {
		log.WithError(err).Error("PinotClient.ListDatabases() failed.")
		return newOkResponse[[]string](nil)
	} else if err != nil {
		return newInternalServerErrorResponse[[]string](err)
	}
	return newOkResponse(databases)
}

func ListTables(client *pinotlib.PinotClient, r *http.Request) *Response[[]string] {
	tables, err := client.ListTables(r.Context())
	if err != nil {
		return newInternalServerErrorResponse[[]string](err)
	}
	return newOkResponse(tables)
}

type GetTablesResponse struct {
	Tables []string `json:"tables"`
}

func GetTableSchema(client *pinotlib.PinotClient, r *http.Request) *Response[pinotlib.TableSchema] {
	vars := mux.Vars(r)
	table := vars["table"]

	schema, err := client.GetTableSchema(r.Context(), table)
	if err != nil {
		return newInternalServerErrorResponse[pinotlib.TableSchema](err)
	}
	return newOkResponse(schema)
}

type PreviewSqlBuilderRequest struct {
	TimeRange           dataquery.TimeRange         `json:"timeRange"`
	IntervalSize        string                      `json:"intervalSize"`
	TableName           string                      `json:"tableName"`
	TimeColumn          string                      `json:"timeColumn"`
	MetricColumn        dataquery.ComplexField      `json:"metricColumn"`
	GroupByColumns      []dataquery.ComplexField    `json:"groupByColumns"`
	AggregationFunction string                      `json:"aggregationFunction"`
	DimensionFilters    []dataquery.DimensionFilter `json:"filters"`
	Limit               int64                       `json:"limit"`
	Granularity         string                      `json:"granularity"`
	OrderByClauses      []dataquery.OrderByClause   `json:"orderBy"`
	QueryOptions        []dataquery.QueryOption     `json:"queryOptions"`
	ExpandMacros        bool                        `json:"expandMacros"`
}

func PreviewSqlBuilder(client *pinotlib.PinotClient, ctx context.Context, data PreviewSqlBuilderRequest) *Response[string] {
	if data.TableName == "" {
		return newOkResponse("")
	}

	query := dataquery.TimeSeriesBuilderQuery{
		TimeRange:           data.TimeRange,
		IntervalSize:        parseIntervalSize(data.IntervalSize),
		TableName:           data.TableName,
		TimeColumn:          data.TimeColumn,
		MetricColumn:        data.MetricColumn,
		GroupByColumns:      data.GroupByColumns,
		AggregationFunction: data.AggregationFunction,
		DimensionFilters:    data.DimensionFilters,
		Limit:               data.Limit,
		Granularity:         data.Granularity,
		OrderByClauses:      data.OrderByClauses,
		QueryOptions:        data.QueryOptions,
	}

	if !data.ExpandMacros {
		sql, err := query.RenderSqlWithMacros()
		if err != nil {
			log.WithError(err).FromContext(ctx).Error("RenderSqlWithMacros() failed.")
			return newOkResponse("")
		}
		return newOkResponse(sql)
	}

	sqlQuery, _, err := query.RenderSqlQuery(ctx, client)
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("RenderTimeSeriesSql() failed.")
		return newOkResponse("")
	}
	return newOkResponse(client.RenderSql(sqlQuery))
}

type PreviewLogsBuilderSqlRequest struct {
	TimeRange        dataquery.TimeRange         `json:"timeRange"`
	TableName        string                      `json:"tableName"`
	TimeColumn       string                      `json:"timeColumn"`
	LogColumn        dataquery.ComplexField      `json:"logColumn"`
	LogColumnAlias   string                      `json:"logColumnAlias"`
	MetadataColumns  []dataquery.ComplexField    `json:"metadataColumns"`
	JsonExtractors   []dataquery.JsonExtractor   `json:"jsonExtractors"`
	RegexpExtractors []dataquery.RegexpExtractor `json:"regexpExtractors"`
	DimensionFilters []dataquery.DimensionFilter `json:"dimensionFilters"`
	QueryOptions     []dataquery.QueryOption     `json:"queryOptions"`
	Limit            int64                       `json:"limit"`
	ExpandMacros     bool                        `json:"expandMacros"`
}

func PreviewLogsSql(client *pinotlib.PinotClient, ctx context.Context, data PreviewLogsBuilderSqlRequest) *Response[string] {
	if data.TableName == "" {
		return newOkResponse("")
	}

	query := dataquery.LogsBuilderQuery{
		TimeRange:        data.TimeRange,
		TableName:        data.TableName,
		TimeColumn:       data.TimeColumn,
		LogColumn:        data.LogColumn,
		LogColumnAlias:   data.LogColumnAlias,
		MetadataColumns:  data.MetadataColumns,
		JsonExtractors:   data.JsonExtractors,
		RegexpExtractors: data.RegexpExtractors,
		DimensionFilters: data.DimensionFilters,
		QueryOptions:     data.QueryOptions,
		Limit:            data.Limit,
	}

	if !data.ExpandMacros {
		sql, err := query.RenderSqlWithMacros()
		if err != nil {
			log.WithError(err).FromContext(ctx).Error("RenderSqlWithMacros() failed.")
			return newOkResponse("")
		}
		return newOkResponse(sql)
	}

	sqlQuery, err := query.RenderSqlQuery(ctx, client)
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("RenderSqlQuery() failed.")
		return newOkResponse("")
	}
	return newOkResponse(client.RenderSql(sqlQuery))
}

type PreviewSqlCodeRequest struct {
	TimeRange         dataquery.TimeRange `json:"timeRange"`
	IntervalSize      string              `json:"intervalSize"`
	TableName         string              `json:"tableName"`
	TimeColumnAlias   string              `json:"timeColumnAlias"`
	TimeColumnFormat  string              `json:"timeColumnFormat"`
	MetricColumnAlias string              `json:"metricColumnAlias"`
	Code              string              `json:"code"`
}

func PreviewSqlCode(client *pinotlib.PinotClient, ctx context.Context, data PreviewSqlCodeRequest) *Response[string] {
	if data.TableName == "" {
		log.FromContext(ctx).Info("Received code preview request without table selection")
		return newOkResponse("")
	}

	query := dataquery.PinotQlCodeQuery{
		TableName:         data.TableName,
		TimeRange:         data.TimeRange,
		IntervalSize:      parseIntervalSize(data.IntervalSize),
		TimeColumnAlias:   data.TimeColumnAlias,
		MetricColumnAlias: data.MetricColumnAlias,
		Code:              data.Code,
	}

	sql, err := query.RenderSqlQuery(ctx, client)
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("RenderPinotSql() failed.")
		return newOkResponse("")
	}
	return newOkResponse(client.RenderSql(sql))
}

type QueryDistinctValuesRequest struct {
	TableName        string                      `json:"tableName"`
	ColumnName       string                      `json:"columnName"`
	ColumnKey        string                      `json:"columnKey"`
	TimeRange        *dataquery.TimeRange        `json:"timeRange"`
	TimeColumn       string                      `json:"timeColumn"`
	DimensionFilters []dataquery.DimensionFilter `json:"filters"`
}

func QueryDistinctValues(client *pinotlib.PinotClient, ctx context.Context, data QueryDistinctValuesRequest) *Response[[]string] {
	sql, err := getDistinctValuesSql(client, ctx, data)
	if err != nil {
		return newInternalServerErrorResponse[[]string](err)
	}
	if sql == "" {
		return newOkResponse[[]string](nil)
	}

	results, err := client.ExecuteSqlQuery(ctx, pinotlib.NewSqlQuery(sql))
	if err != nil {
		return newInternalServerErrorResponse[[]string](err)
	}

	var valueExprs []string
	if results.HasData() {
		valueExprs = pinotlib.ExtractColumnAsExprs(results.ResultTable, 0)
	}
	return newOkResponse(valueExprs)
}

type PreviewSqlDistinctValuesRequest QueryDistinctValuesRequest

func PreviewSqlDistinctValues(client *pinotlib.PinotClient, ctx context.Context, data PreviewSqlDistinctValuesRequest) *Response[string] {
	sql, err := getDistinctValuesSql(client, ctx, QueryDistinctValuesRequest(data))
	if err != nil {
		return newErrorResponse[string](http.StatusInternalServerError, err)
	}

	return newOkResponse(sql)
}

func getDistinctValuesSql(client *pinotlib.PinotClient, ctx context.Context, data QueryDistinctValuesRequest) (string, error) {
	if data.TableName == "" || data.ColumnName == "" {
		return "", nil
	}

	var timeFilterExpr pinotlib.SqlExpr
	if data.TimeRange != nil {
		tableSchema, err := client.GetTableSchema(ctx, data.TableName)
		if err != nil {
			return "", err
		}

		format, err := pinotlib.GetTimeColumnFormat(tableSchema, data.TimeColumn)
		if err != nil {
			return "", err
		}

		timeFilterExpr = pinotlib.TimeFilterExpr(pinotlib.TimeFilter{
			Column: data.TimeColumn,
			Format: format,
			From:   data.TimeRange.From,
			To:     data.TimeRange.To,
		})
	}

	return pinotlib.RenderDistinctValuesSql(pinotlib.DistinctValuesSqlParams{
		ColumnExpr:           pinotlib.ComplexFieldExpr(data.ColumnName, data.ColumnKey),
		TableName:            data.TableName,
		TimeFilterExpr:       timeFilterExpr,
		DimensionFilterExprs: dataquery.FilterExprsFrom(data.DimensionFilters),
	})
}

func ListTimeSeriesTables(client *pinotlib.PinotClient, r *http.Request) *Response[[]string] {
	tables, err := client.ListTimeSeriesTables(r.Context())
	if err != nil {
		return newInternalServerErrorResponse[[]string](err)
	}
	return newOkResponse(tables)
}

type ListTimeSeriesMetricsRequest struct {
	TableName string              `json:"tableName"`
	TimeRange dataquery.TimeRange `json:"timeRange"`
}

func ListTimeSeriesMetrics(client *pinotlib.PinotClient, ctx context.Context, data ListTimeSeriesMetricsRequest) *Response[[]string] {
	if data.TableName == "" {
		return newBadRequestResponse[[]string](errors.New("tableName is required"))
	} else if ok, err := client.IsTimeSeriesTable(ctx, data.TableName); err != nil {
		return newInternalServerErrorResponse[[]string](err)
	} else if !ok {
		return newBadRequestResponse[[]string](fmt.Errorf("table `%s` is not a time series table", data.TableName))
	}

	metrics, err := client.ListTimeSeriesMetrics(ctx, pinotlib.TimeSeriesMetricNamesQuery{
		TableName: data.TableName,
		From:      data.TimeRange.From,
		To:        data.TimeRange.To,
	})
	if err != nil {
		return newInternalServerErrorResponse[[]string](err)
	}
	return newOkResponse(metrics)
}

type ListTimeSeriesLabelsRequest = struct {
	TableName  string              `json:"tableName"`
	MetricName string              `json:"metricName"`
	TimeRange  dataquery.TimeRange `json:"timeRange"`
}

func ListTimeSeriesLabels(client *pinotlib.PinotClient, ctx context.Context, data ListTimeSeriesLabelsRequest) *Response[[]string] {
	if data.TableName == "" {
		return newBadRequestResponse[[]string](errors.New("tableName is required"))
	} else if ok, err := client.IsTimeSeriesTable(ctx, data.TableName); err != nil {
		return newInternalServerErrorResponse[[]string](err)
	} else if !ok {
		return newBadRequestResponse[[]string](fmt.Errorf("table `%s` is not a time series table", data.TableName))
	}

	labels, err := client.ListTimeSeriesLabelNames(ctx, pinotlib.TimeSeriesLabelNamesQuery{
		TableName:  data.TableName,
		MetricName: data.MetricName,
		From:       data.TimeRange.From,
		To:         data.TimeRange.To,
	})
	if err != nil {
		return newInternalServerErrorResponse[[]string](err)
	}
	return newOkResponse(labels)
}

type ListTimeSeriesLabelValuesRequest struct {
	TableName  string              `json:"tableName"`
	MetricName string              `json:"metricName"`
	LabelName  string              `json:"labelName"`
	TimeRange  dataquery.TimeRange `json:"timeRange"`
}

func ListTimeSeriesLabelValues(client *pinotlib.PinotClient, ctx context.Context, data ListTimeSeriesLabelValuesRequest) *Response[[]string] {
	if data.TableName == "" {
		return newBadRequestResponse[[]string](errors.New("tableName is required"))
	} else if data.LabelName == "" {
		return newBadRequestResponse[[]string](errors.New("labelName is required"))
	} else if ok, err := client.IsTimeSeriesTable(ctx, data.TableName); err != nil {
		return newInternalServerErrorResponse[[]string](err)
	} else if !ok {
		return newBadRequestResponse[[]string](fmt.Errorf("table `%s` is not a time series table", data.TableName))
	}

	values, err := client.ListTimeSeriesLabelValues(ctx, pinotlib.TimeSeriesLabelValuesQuery{
		TableName:  data.TableName,
		MetricName: data.MetricName,
		LabelName:  data.LabelName,
		From:       data.TimeRange.From,
		To:         data.TimeRange.To,
	})
	if err != nil {
		return newInternalServerErrorResponse[[]string](err)
	}
	return newOkResponse(values)
}

func IsPromQlSupported(client *pinotlib.PinotClient, r *http.Request) *Response[bool] {
	ok, err := client.IsTimeseriesSupported(r.Context())
	if err != nil {
		return newInternalServerErrorResponse[bool](err)
	}
	return newOkResponse(ok)
}

type ListSuggestedGranularitiesRequest = struct {
	TableName  string `json:"tableName"`
	TimeColumn string `json:"timeColumn"`
}

type Granularity struct {
	Name      string  `json:"name"`
	Optimized bool    `json:"optimized"`
	Seconds   float64 `json:"seconds"`
}

var commonGranularities = []Granularity{
	{Name: "auto", Optimized: false, Seconds: 0},
	{Name: "MILLISECONDS", Optimized: false, Seconds: 0.001},
	{Name: "SECONDS", Optimized: false, Seconds: 1},
	{Name: "MINUTES", Optimized: false, Seconds: 60},
	{Name: "HOURS", Optimized: false, Seconds: 3600},
	{Name: "DAYS", Optimized: false, Seconds: 86400},
}

func ListSuggestedGranularities(client *pinotlib.PinotClient, ctx context.Context, req ListSuggestedGranularitiesRequest) *Response[[]Granularity] {
	if req.TableName == "" || req.TimeColumn == "" {
		return newOkResponse(commonGranularities)
	}

	schema, err := client.GetTableSchema(ctx, req.TableName)
	if err != nil {
		return newInternalServerErrorResponse[[]Granularity](err)
	}

	timeColumnFormat, err := pinotlib.GetTimeColumnFormat(schema, req.TimeColumn)
	if err != nil {
		return newInternalServerErrorResponse[[]Granularity](err)
	}
	minPinotGranularity := timeColumnFormat.MinimumGranularity()

	configs, err := client.ListTableConfigs(ctx, req.TableName)
	if err != nil {
		return newInternalServerErrorResponse[[]Granularity](err)
	}

	distinctSuggestions := make(map[float64]Granularity)
	for _, granularity := range commonGranularities {
		if granularity.Seconds >= minPinotGranularity.Duration().Seconds() || granularity.Name == "auto" {
			distinctSuggestions[granularity.Seconds] = granularity
		}
	}

	derivedGranularities := pinotlib.DerivedGranularitiesFor(configs, req.TimeColumn, dataquery.OutputTimeFormat())
	for _, pinotGranularity := range derivedGranularities {
		distinctSuggestions[pinotGranularity.Duration().Seconds()] = Granularity{
			Name:      pinotGranularity.ShortString(),
			Optimized: true,
			Seconds:   pinotGranularity.Duration().Seconds(),
		}
	}

	if timeColumnFormat.Equals(dataquery.OutputTimeFormat()) {
		distinctSuggestions[minPinotGranularity.Duration().Seconds()] = Granularity{
			Name:      minPinotGranularity.ShortString(),
			Optimized: true,
			Seconds:   minPinotGranularity.Duration().Seconds(),
		}
	}

	results := make([]Granularity, 0, len(distinctSuggestions))
	for _, granularity := range distinctSuggestions {
		results = append(results, granularity)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Seconds < results[j].Seconds })

	return newOkResponse(results)
}

type ListColumnsRequest struct {
	TableName        string                      `json:"tableName"`
	TimeRange        *dataquery.TimeRange        `json:"timeRange"`
	TimeColumn       string                      `json:"timeColumn"`
	DimensionFilters []dataquery.DimensionFilter `json:"filters"`
}

type Column = struct {
	Name      string `json:"name"`
	Key       string `json:"key,omitempty"`
	DataType  string `json:"dataType"`
	IsTime    bool   `json:"isTime,omitempty"`
	IsMetric  bool   `json:"isMetric,omitempty"`
	IsDerived bool   `json:"isDerived,omitempty"`
}

func ListColumns(client *pinotlib.PinotClient, ctx context.Context, req ListColumnsRequest) *Response[[]Column] {
	if req.TableName == "" {
		return newOkResponse[[]Column](nil)
	}

	schema, err := client.GetTableSchema(ctx, req.TableName)
	if err != nil {
		return newInternalServerErrorResponse[[]Column](err)
	}

	tableConfigs, err := client.ListTableConfigs(ctx, req.TableName)
	if err != nil {
		return newInternalServerErrorResponse[[]Column](err)
	}

	derivedTimeCols := collections.NewSet[string](0)
	for _, col := range pinotlib.DerivedTimeColumnsFrom(tableConfigs) {
		derivedTimeCols.Add(col.ColumnName)
	}

	var columns []Column

	for _, spec := range schema.DateTimeFieldSpecs {
		columns = append(columns, Column{
			Name:      spec.Name,
			DataType:  spec.DataType,
			IsTime:    true,
			IsDerived: derivedTimeCols.Contains(spec.Name),
		})
	}
	for _, spec := range schema.DimensionFieldSpecs {
		columns = append(columns, Column{
			Name:     spec.Name,
			DataType: spec.DataType,
			IsMetric: pinotlib.IsNumericDataType(spec.DataType),
		})
	}
	for _, spec := range schema.MetricFieldSpecs {
		columns = append(columns, Column{
			Name:     spec.Name,
			DataType: spec.DataType,
			IsMetric: pinotlib.IsNumericDataType(spec.DataType),
		})
	}
	if len(schema.ComplexFieldSpecs) == 0 {
		return newOkResponse(columns)
	}

	// Complex fields

	format, err := pinotlib.GetTimeColumnFormat(schema, req.TimeColumn)
	if err != nil {
		return newOkResponse(columns)
	}

	timeFilterExpr := pinotlib.TimeFilterExpr(pinotlib.TimeFilter{
		Column: req.TimeColumn,
		Format: format,
		From:   req.TimeRange.From,
		To:     req.TimeRange.To,
	})
	filterExprs := dataquery.FilterExprsFrom(req.DimensionFilters)
	for _, spec := range schema.ComplexFieldSpecs {
		keys := listMapColumnKeys(client, ctx, req.TableName, spec.Name, timeFilterExpr, filterExprs)
		for _, key := range keys {
			dataType := spec.ChildFieldSpecs.Value.DataType
			columns = append(columns, Column{
				Name:     spec.Name,
				Key:      key,
				DataType: dataType,
				IsMetric: pinotlib.IsNumericDataType(dataType),
			})
		}
	}
	return newOkResponse(columns)
}

func listMapColumnKeys(client *pinotlib.PinotClient, ctx context.Context, tableName string, columnName string, timeFilterExpr pinotlib.SqlExpr, filterExprs []pinotlib.SqlExpr) []string {
	columnExpr := pinotlib.CastExpr(pinotlib.ObjectExpr(columnName), pinotlib.DataTypeJson)
	sql, _ := pinotlib.RenderDistinctValuesSql(pinotlib.DistinctValuesSqlParams{
		ColumnExpr:           columnExpr,
		TableName:            tableName,
		TimeFilterExpr:       timeFilterExpr,
		DimensionFilterExprs: filterExprs,
	})

	results, err := client.ExecuteSqlQuery(ctx, pinotlib.NewSqlQuery(sql))
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("Query to extract map keys failed")
		return nil
	} else if !results.HasData() {
		log.FromContext(ctx).Debug("Query to extract map keys returned no data")
		return nil
	}

	col, err := pinotlib.DecodeJsonFromColumn[map[string]any](results.ResultTable, 0)
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("Query to extract map keys returned column with invalid json")
		return nil
	}

	keys := collections.NewSet[string](0)
	for _, entry := range col {
		for k := range entry {
			keys.Add(k)
		}
	}
	values := keys.Values()
	sort.Strings(values)
	return values
}

func newOkResponse[T any](result T) *Response[T] {
	return &Response[T]{Code: http.StatusOK, Result: result}
}

func newBadRequestResponse[T any](err error) *Response[T] {
	return newErrorResponse[T](http.StatusBadRequest, err)
}

func newInternalServerErrorResponse[T any](err error) *Response[T] {
	return newErrorResponse[T](http.StatusInternalServerError, err)
}

func newErrorResponse[T any](code int, err error) *Response[T] {
	return &Response[T]{Code: code, Error: err.Error()}
}

func adaptHandler[T any](client *pinotlib.PinotClient, handler func(*pinotlib.PinotClient, *http.Request) *Response[T]) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		startTime := time.Now()
		thisClient := auth.PassThroughOAuth(client, req)
		resp := handler(thisClient, req)
		writeResponse(w, resp)
		captureMetrics(startTime, req, resp)
	}
}

func adaptHandlerWithBody[TIn any, TOut any](client *pinotlib.PinotClient, handler func(*pinotlib.PinotClient, context.Context, TIn) *Response[TOut]) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		startTime := time.Now()
		thisClient := auth.PassThroughOAuth(client, req)

		var data TIn
		var resp *Response[TOut]
		if err := json.NewDecoder(req.Body).Decode(&data); err != nil {
			resp = newBadRequestResponse[TOut](err)
		} else {
			resp = handler(thisClient, req.Context(), data)
		}

		writeResponse(w, resp)
		captureMetrics(startTime, req, resp)
	}
}

func captureMetrics[TOut any](startTime time.Time, req *http.Request, resp *Response[TOut]) {
	labels := prometheus.Labels{
		"endpoint": req.URL.Path,
		"status":   strconv.FormatInt(int64(resp.Code), 10),
	}
	requestCounter.With(labels).Inc()
	requestDuration.With(labels).Observe(time.Since(startTime).Seconds())
}

func parseIntervalSize(intervalSize string) time.Duration {
	if intervalSize == "1d" {
		return time.Second * 24 * 3600
	}
	interval, _ := time.ParseDuration(intervalSize)
	return interval
}

func writeResponse[T any](w http.ResponseWriter, resp *Response[T]) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.Code)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.WithError(err).Error("Failed to write http response")
	}
}
