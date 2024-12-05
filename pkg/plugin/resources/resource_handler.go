package resources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/collections"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/dataquery"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/templates"
	"net/http"
	"sort"
	"time"
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
	router.HandleFunc("/preview/sql/code", adaptHandlerWithBody(handler.PreviewSqlCode))
	router.HandleFunc("/preview/sql/distinctValues", adaptHandlerWithBody(handler.PreviewSqlDistinctValues))
	router.HandleFunc("/query/distinctValues", adaptHandlerWithBody(handler.QueryDistinctValues))
	router.HandleFunc("/tables", adaptHandler(handler.ListTables))
	router.HandleFunc("/tables/{table}/schema", adaptHandler(handler.GetTableSchema))
	router.HandleFunc("/tables/{table}/timeColumns", adaptHandler(handler.ListTimeColumns))
	router.HandleFunc("/timeseries/tables", adaptHandler(handler.ListTimeSeriesTables))
	router.HandleFunc("/timeseries/metrics", adaptHandlerWithBody(handler.ListTimeSeriesMetrics))
	router.HandleFunc("/timeseries/labels", adaptHandlerWithBody(handler.ListTimeSeriesLabels))
	router.HandleFunc("/timeseries/labelValues", adaptHandlerWithBody(handler.ListTimeSeriesLabelValues))
	router.HandleFunc("/granularities", adaptHandlerWithBody(handler.ListSuggestedGranularities))

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
	DatabaseName        string                      `json:"databaseName"`
	TableName           string                      `json:"tableName"`
	TimeColumn          string                      `json:"timeColumn"`
	MetricColumn        string                      `json:"metricColumn"`
	GroupByColumns      []string                    `json:"groupByColumns"`
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

	driver, err := dataquery.NewPinotQlBuilderDriver(dataquery.PinotQlBuilderParams{
		PinotClient:         x.client,
		TableSchema:         tableSchema,
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
	})
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("Failed to build query driver.")
		return newOkResponse("")

	}

	sql, err := driver.RenderPinotSql(data.ExpandMacros)
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("PinotDriver.RenderSql() failed.")
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

	driver, err := dataquery.NewPinotQlCodeDriver(dataquery.PinotQlCodeDriverParams{
		PinotClient:       x.client,
		TableName:         data.TableName,
		TimeRange:         data.TimeRange,
		IntervalSize:      parseIntervalSize(data.IntervalSize),
		TableSchema:       tableSchema,
		TimeColumnAlias:   data.TimeColumnAlias,
		MetricColumnAlias: data.MetricColumnAlias,
		Code:              data.Code,
	})
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("NewPinotQlCodeDriver() failed.")
		return newOkResponse("")
	}

	sql, err := driver.RenderPinotSql()
	if err != nil {
		log.WithError(err).FromContext(ctx).Error("RenderPinotSql() failed.")
		return newOkResponse("")
	}
	return newOkResponse(sql)
}

type QueryDistinctValuesRequest struct {
	TableName        string                      `json:"tableName"`
	ColumnName       string                      `json:"columnName"`
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
		ColumnName:           data.ColumnName,
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

	derivedGranularities := pinotlib.DerivedGranularitiesFor(configs, req.TimeColumn, dataquery.TimeOutputFormat())
	for _, pinotGranularity := range derivedGranularities {
		distinctSuggestions[pinotGranularity.Duration().Seconds()] = Granularity{
			Name:      pinotGranularity.ShortString(),
			Optimized: true,
			Seconds:   pinotGranularity.Duration().Seconds(),
		}
	}

	if timeColumnFormat.Equals(dataquery.TimeOutputFormat()) {
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

type TimeColumn struct {
	Name                    string `json:"name"`
	IsDerived               bool   `json:"isDerived"`
	HasDerivedGranularities bool   `json:"hasDerivedGranularities"`
}

func (x *ResourceHandler) ListTimeColumns(r *http.Request) *Response[[]TimeColumn] {
	vars := mux.Vars(r)
	table := vars["table"]

	schema, err := x.client.GetTableSchema(r.Context(), table)
	if err != nil {
		return newInternalServerErrorResponse[[]TimeColumn](err)
	}

	tableConfigs, err := x.client.ListTableConfigs(r.Context(), table)
	if err != nil {
		return newInternalServerErrorResponse[[]TimeColumn](err)
	}

	derivedTimeCols := collections.NewSet[string](0)
	for _, col := range pinotlib.DerivedTimeColumnsFrom(tableConfigs) {
		derivedTimeCols.Add(col.ColumnName)
	}

	colsWithDerivedGranularities := collections.NewSet[string](0)
	for _, col := range pinotlib.DerivedTimeColumnsFrom(tableConfigs) {
		colsWithDerivedGranularities.Add(col.Source.TimeColumn)
	}

	results := make([]TimeColumn, len(schema.DateTimeFieldSpecs))
	for i, col := range schema.DateTimeFieldSpecs {
		results[i] = TimeColumn{
			Name:                    col.Name,
			IsDerived:               derivedTimeCols.Contains(col.Name),
			HasDerivedGranularities: colsWithDerivedGranularities.Contains(col.Name),
		}
	}

	sort.Slice(results, func(i, j int) bool { return results[i].Name < results[j].Name })
	return newOkResponse(results)
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

func adaptHandler[T any](handler func(r *http.Request) *Response[T]) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeResponse(w, handler(r))
	}
}

func adaptHandlerWithBody[I any, O any](handler func(ctx context.Context, data I) *Response[O]) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var data I
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			writeResponse(w, newBadRequestResponse[O](err))
			return
		}
		writeResponse(w, handler(r.Context(), data))
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
