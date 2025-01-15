package resources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/collections"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/dataquery"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/templates"
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

type ResourceHandler struct {
	client *pinotlib.PinotClient
}

func NewPinotResourceHandler(client *pinotlib.PinotClient) http.Handler {
	router := mux.NewRouter()

	handler := ResourceHandler{client: client}
	router.HandleFunc("/databases", adaptHandler(handler.ListDatabases))
	router.HandleFunc("/isPromQlSupported", adaptHandler(handler.IsPromQlSupported))
	router.HandleFunc("/preview/sql/builder", adaptHandlerWithBody(handler.PreviewSqlBuilder))
	router.HandleFunc("/preview/logs/sql", adaptHandlerWithBody(handler.PreviewLogsSql))
	router.HandleFunc("/preview/sql/code", adaptHandlerWithBody(handler.PreviewSqlCode))
	router.HandleFunc("/preview/sql/distinctValues", adaptHandlerWithBody(handler.PreviewSqlDistinctValues))
	router.HandleFunc("/query/distinctValues", adaptHandlerWithBody(handler.QueryDistinctValues))
	router.HandleFunc("/tables", adaptHandler(handler.ListTables))
	router.HandleFunc("/tables/{table}/schema", adaptHandler(handler.GetTableSchema))
	router.HandleFunc("/timeseries/tables", adaptHandler(handler.ListTimeSeriesTables))
	router.HandleFunc("/timeseries/metrics", adaptHandlerWithBody(handler.ListTimeSeriesMetrics))
	router.HandleFunc("/timeseries/labels", adaptHandlerWithBody(handler.ListTimeSeriesLabels))
	router.HandleFunc("/timeseries/labelValues", adaptHandlerWithBody(handler.ListTimeSeriesLabelValues))
	router.HandleFunc("/granularities", adaptHandlerWithBody(handler.ListSuggestedGranularities))
	router.HandleFunc("/columns", adaptHandlerWithBody(handler.ListColumns))
	return router
}

type Response[T any] struct {
	Code   int    `json:"code"`
	Error  string `json:"error,omitempty"`
	Result T      `json:"result,omitempty"`
}

func (x *ResourceHandler) ListDatabases(r *http.Request) *Response[[]string] {
	databases, err := x.client.ListDatabases(r.Context())

	if pinotlib.IsStatusForbiddenError(err) {
		log.WithError(err).Error("PinotClient.ListDatabases() failed.")
		return newOkResponse[[]string](nil)
	} else if err != nil {
		return newInternalServerErrorResponse[[]string](err)
	}
	return newOkResponse(databases)
}

func (x *ResourceHandler) ListTables(r *http.Request) *Response[[]string] {
	tables, err := x.client.ListTables(r.Context())
	if err != nil {
		return newInternalServerErrorResponse[[]string](err)
	}
	return newOkResponse(tables)
}

type GetTablesResponse struct {
	Tables []string `json:"tables"`
}

func (x *ResourceHandler) GetTableSchema(r *http.Request) *Response[pinotlib.TableSchema] {
	vars := mux.Vars(r)
	table := vars["table"]

	schema, err := x.client.GetTableSchema(r.Context(), table)
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

func (x *ResourceHandler) PreviewSqlBuilder(ctx context.Context, data PreviewSqlBuilderRequest) *Response[string] {
	if data.TableName == "" {
		return newOkResponse("")
	}

	tableSchema, err := x.client.GetTableSchema(ctx, data.TableName)
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("PinotClient.GetTableSchema() failed.")
		return newOkResponse("")
	}

	tableConfigs, err := x.client.ListTableConfigs(ctx, data.TableName)
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("PinotClient.ListTableConfigs() failed.")
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

	var sql string
	if data.ExpandMacros {
		sql, err = query.RenderSql(ctx, tableSchema, tableConfigs)
	} else {
		sql, err = query.RenderSqlWithMacros(ctx, tableSchema, tableConfigs)
	}
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("RenderTimeSeriesSql() failed.")
		return newOkResponse("")
	}

	return newOkResponse(sql)
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

func (x *ResourceHandler) PreviewLogsSql(ctx context.Context, data PreviewLogsBuilderSqlRequest) *Response[string] {
	if data.TableName == "" {
		return newOkResponse("")
	}

	tableSchema, err := x.client.GetTableSchema(ctx, data.TableName)
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("PinotClient.GetTableSchema() failed.")
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

	var sql string
	if data.ExpandMacros {
		sql, err = query.RenderSql(tableSchema)
	} else {
		sql, err = query.RenderSqlWithMacros()
	}

	if err != nil {
		log.WithError(err).FromContext(ctx).Error("RenderLogsBuilderSql() failed.")
		return newOkResponse("")
	}
	return newOkResponse(sql)
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

func (x *ResourceHandler) PreviewSqlCode(ctx context.Context, data PreviewSqlCodeRequest) *Response[string] {
	if data.TableName == "" {
		log.FromContext(ctx).Info("Received code preview request without table selection")
		return newOkResponse("")
	}

	tableSchema, err := x.client.GetTableSchema(ctx, data.TableName)
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("PinotClient.GetTableSchema() failed.")
		return newOkResponse("")
	}

	tableConfigs, err := x.client.ListTableConfigs(ctx, data.TableName)
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("PinotClient.ListTableConfigs() failed.")
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

	sql, err := query.RenderSql(ctx, tableSchema, tableConfigs)
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("RenderPinotSql() failed.")
		return newOkResponse("")
	}
	return newOkResponse(sql)
}

type QueryDistinctValuesRequest struct {
	TableName        string                      `json:"tableName"`
	ColumnName       string                      `json:"columnName"`
	ColumnKey        string                      `json:"columnKey"`
	TimeRange        *dataquery.TimeRange        `json:"timeRange"`
	TimeColumn       string                      `json:"timeColumn"`
	DimensionFilters []dataquery.DimensionFilter `json:"filters"`
}

func (x *ResourceHandler) QueryDistinctValues(ctx context.Context, data QueryDistinctValuesRequest) *Response[[]string] {
	sql, err := x.getDistinctValuesSql(ctx, data)
	if err != nil {
		return newInternalServerErrorResponse[[]string](err)
	}
	if sql == "" {
		return newOkResponse[[]string](nil)
	}

	results, err := x.client.ExecuteSqlQuery(ctx, pinotlib.NewSqlQuery(sql))
	if err != nil {
		return newInternalServerErrorResponse[[]string](err)
	}

	var valueExprs []string
	if results.HasData() {
		valueExprs = pinotlib.ExtractColumnAsExprs(results.ResultTable, 0)
	}
	return newOkResponse(valueExprs)
}

type PreviewSqlDistinctValues QueryDistinctValuesRequest

func (x *ResourceHandler) PreviewSqlDistinctValues(ctx context.Context, data PreviewSqlDistinctValues) *Response[string] {
	sql, err := x.getDistinctValuesSql(ctx, QueryDistinctValuesRequest(data))
	if err != nil {
		return newErrorResponse[string](http.StatusInternalServerError, err)
	}

	return newOkResponse(sql)
}

func (x *ResourceHandler) getDistinctValuesSql(ctx context.Context, data QueryDistinctValuesRequest) (string, error) {
	if data.TableName == "" || data.ColumnName == "" {
		return "", nil
	}

	var timeFilterExpr string
	if data.TimeRange != nil {
		tableSchema, err := x.client.GetTableSchema(ctx, data.TableName)
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

	return templates.RenderDistinctValuesSql(templates.DistinctValuesSqlParams{
		ColumnExpr:           pinotlib.ComplexFieldExpr(data.ColumnName, data.ColumnKey),
		TableName:            data.TableName,
		TimeFilterExpr:       timeFilterExpr,
		DimensionFilterExprs: dataquery.FilterExprsFrom(data.DimensionFilters),
	})
}

func (x *ResourceHandler) ListTimeSeriesTables(r *http.Request) *Response[[]string] {
	tables, err := x.client.ListTimeSeriesTables(r.Context())
	if err != nil {
		return newInternalServerErrorResponse[[]string](err)
	}
	return newOkResponse(tables)
}

type ListTimeSeriesMetricsRequest struct {
	TableName string              `json:"tableName"`
	TimeRange dataquery.TimeRange `json:"timeRange"`
}

func (x *ResourceHandler) ListTimeSeriesMetrics(ctx context.Context, data ListTimeSeriesMetricsRequest) *Response[[]string] {
	if data.TableName == "" {
		return newBadRequestResponse[[]string](errors.New("tableName is required"))
	} else if ok, err := x.client.IsTimeSeriesTable(ctx, data.TableName); err != nil {
		return newInternalServerErrorResponse[[]string](err)
	} else if !ok {
		return newBadRequestResponse[[]string](fmt.Errorf("table `%s` is not a time series table", data.TableName))
	}

	metrics, err := x.client.ListTimeSeriesMetrics(ctx, pinotlib.TimeSeriesMetricNamesQuery{
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

func (x *ResourceHandler) ListTimeSeriesLabels(ctx context.Context, data ListTimeSeriesLabelsRequest) *Response[[]string] {
	if data.TableName == "" {
		return newBadRequestResponse[[]string](errors.New("tableName is required"))
	} else if ok, err := x.client.IsTimeSeriesTable(ctx, data.TableName); err != nil {
		return newInternalServerErrorResponse[[]string](err)
	} else if !ok {
		return newBadRequestResponse[[]string](fmt.Errorf("table `%s` is not a time series table", data.TableName))
	}

	labels, err := x.client.ListTimeSeriesLabelNames(ctx, pinotlib.TimeSeriesLabelNamesQuery{
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

func (x *ResourceHandler) ListTimeSeriesLabelValues(ctx context.Context, data ListTimeSeriesLabelValuesRequest) *Response[[]string] {
	if data.TableName == "" {
		return newBadRequestResponse[[]string](errors.New("tableName is required"))
	} else if data.LabelName == "" {
		return newBadRequestResponse[[]string](errors.New("labelName is required"))
	} else if ok, err := x.client.IsTimeSeriesTable(ctx, data.TableName); err != nil {
		return newInternalServerErrorResponse[[]string](err)
	} else if !ok {
		return newBadRequestResponse[[]string](fmt.Errorf("table `%s` is not a time series table", data.TableName))
	}

	values, err := x.client.ListTimeSeriesLabelValues(ctx, pinotlib.TimeSeriesLabelValuesQuery{
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

func (x *ResourceHandler) IsPromQlSupported(r *http.Request) *Response[bool] {
	ok, err := x.client.IsTimeseriesSupported(r.Context())
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

func (x *ResourceHandler) ListSuggestedGranularities(ctx context.Context, req ListSuggestedGranularitiesRequest) *Response[[]Granularity] {
	if req.TableName == "" || req.TimeColumn == "" {
		return newOkResponse(commonGranularities)
	}

	schema, err := x.client.GetTableSchema(ctx, req.TableName)
	if err != nil {
		return newInternalServerErrorResponse[[]Granularity](err)
	}

	timeColumnFormat, err := pinotlib.GetTimeColumnFormat(schema, req.TimeColumn)
	if err != nil {
		return newInternalServerErrorResponse[[]Granularity](err)
	}
	minPinotGranularity := timeColumnFormat.MinimumGranularity()

	configs, err := x.client.ListTableConfigs(ctx, req.TableName)
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

func (x *ResourceHandler) ListColumns(ctx context.Context, req ListColumnsRequest) *Response[[]Column] {
	if req.TableName == "" {
		return newOkResponse[[]Column](nil)
	}

	schema, err := x.client.GetTableSchema(ctx, req.TableName)
	if err != nil {
		return newInternalServerErrorResponse[[]Column](err)
	}

	tableConfigs, err := x.client.ListTableConfigs(ctx, req.TableName)
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
		keys := x.listMapColumnKeys(ctx, req.TableName, spec.Name, timeFilterExpr, filterExprs)
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

func (x *ResourceHandler) listTimeColumns(schema pinotlib.TableSchema, tableConfigs pinotlib.ListTableConfigsResponse) []Column {
	derivedTimeCols := collections.NewSet[string](0)
	for _, col := range pinotlib.DerivedTimeColumnsFrom(tableConfigs) {
		derivedTimeCols.Add(col.ColumnName)
	}

	results := make([]Column, len(schema.DateTimeFieldSpecs))
	for i, col := range schema.DateTimeFieldSpecs {
		results[i] = Column{
			Name:      col.Name,
			IsDerived: derivedTimeCols.Contains(col.Name),
		}
	}
	return results
}

func (x *ResourceHandler) listMapColumnKeys(ctx context.Context, tableName string, columnName string, timeFilterExpr string, filterExprs []string) []string {
	columnExpr := fmt.Sprintf(`CAST(%s AS %s)`, pinotlib.ObjectExpr(columnName), pinotlib.DataTypeJson)
	sql, _ := templates.RenderDistinctValuesSql(templates.DistinctValuesSqlParams{
		ColumnExpr:           columnExpr,
		TableName:            tableName,
		TimeFilterExpr:       timeFilterExpr,
		DimensionFilterExprs: filterExprs,
	})

	results, err := x.client.ExecuteSqlQuery(ctx, pinotlib.NewSqlQuery(sql))
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("Query to extract map keys failed")
		return nil
	}
	col, _ := pinotlib.DecodeJsonFromColumn[map[string]any](results.ResultTable, 0)

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

func adaptHandler[T any](handler func(*http.Request) *Response[T]) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		startTime := time.Now()
		resp := handler(req)
		duration := time.Since(startTime)

		labels := promLabelsFor(req, resp)
		requestCounter.With(labels).Inc()
		requestDuration.With(labels).Observe(duration.Seconds())
		writeResponse(w, resp)
	}
}

func adaptHandlerWithBody[I any, O any](handler func(ctx context.Context, data I) *Response[O]) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		var data I
		if err := json.NewDecoder(req.Body).Decode(&data); err != nil {
			writeResponse(w, newBadRequestResponse[O](err))
			return
		}

		startTime := time.Now()
		resp := handler(req.Context(), data)
		duration := time.Since(startTime)

		labels := promLabelsFor(req, resp)
		requestCounter.With(labels).Inc()
		requestDuration.With(labels).Observe(duration.Seconds())
		writeResponse(w, handler(req.Context(), data))
	}
}

func promLabelsFor[T any](req *http.Request, resp *Response[T]) prometheus.Labels {
	return prometheus.Labels{
		"endpoint": req.URL.Path,
		"status":   strconv.FormatInt(int64(resp.Code), 10),
	}
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
