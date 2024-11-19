package pinotlib

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/collections"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/log"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/templates"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"
)

const TimeSeriesEndpoint = "/timeseries/api/v1"

type TimeSeriesRangeQuery struct {
	Language  string
	Query     string
	Start     time.Time
	End       time.Time
	Step      time.Duration
	TableName string
}

type TimeSeriesQueryResponse struct {
	Status string         `json:"status"`
	Data   TimeSeriesData `json:"data"`
}

type TimeSeriesData struct {
	ResultType string             `json:"resultType"`
	Result     []TimeSeriesResult `json:"result"`
}

type TimeSeriesResult struct {
	Metric     map[string]string
	Timestamps []time.Time
	Values     []float64
}

func (x *TimeSeriesResult) UnmarshalJSON(b []byte) error {
	var data struct {
		Metric map[string]string `json:"metric"`
		Values [][]interface{}   `json:"values"`
	}
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	if err := decoder.Decode(&data); err != nil {
		return err
	}

	x.Metric = flattenLabels(data.Metric)
	x.Timestamps = make([]time.Time, 0, len(data.Values))
	x.Values = make([]float64, 0, len(data.Values))
	for i := range data.Values {
		if len(data.Values[i]) != 2 {
			return fmt.Errorf("expected 2 values got %d", len(data.Values[i]))
		} else if data.Values[i][1] == nil {
			continue
		}

		valueStr := data.Values[i][1].(string)
		value, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return err
		}
		x.Values = append(x.Values, value)

		ts, err := data.Values[i][0].(json.Number).Int64()
		if err != nil {
			return err
		}
		x.Timestamps = append(x.Timestamps, time.Unix(ts, 0).UTC())
	}
	x.Values = x.Values[:]
	x.Timestamps = x.Timestamps[:]
	return nil
}

// TODO: This can be removed once labels are properly handled by Pinot.
func flattenLabels(metric map[string]string) map[string]string {
	labelsJson := metric["labels"]
	if labelsJson == "" {
		return metric
	}

	var labels map[string]string
	err := json.Unmarshal([]byte(metric["labels"]), &labels)
	if err != nil {
		log.WithError(err).Info("Failed to unmarshal time series labels", "labelsJson", labelsJson)
	}
	labels["__name__"] = metric["__name__"]
	return labels
}

func (p *PinotClient) IsTimeseriesSupported(ctx context.Context) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	req, err := p.newRequest(ctx, http.MethodHead, p.properties.BrokerUrl+TimeSeriesEndpoint+"/query_range", nil)
	if err != nil {
		return false, err
	}

	resp, err := p.doRequest(req)
	if err != nil {
		return false, err
	}
	defer p.closeResponseBody(ctx, resp)

	return resp.StatusCode != http.StatusNotFound, nil
}

func (p *PinotClient) ExecuteTimeSeriesQuery(ctx context.Context, req *TimeSeriesRangeQuery) (*TimeSeriesQueryResponse, error) {
	tableMetadata, err := p.GetTableMetadata(ctx, req.TableName)
	if err != nil {
		return nil, err
	}

	formatTime := func(t time.Time) string {
		return strconv.FormatInt(t.Unix(), 10)
	}

	formatStep := func(s time.Duration) string {
		step := int64(math.Max(1, s.Seconds()))
		return strconv.FormatInt(step, 10)
	}

	values := make(url.Values)
	values.Set("language", req.Language)
	values.Set("query", req.Query)
	values.Set("start", formatTime(req.Start))
	values.Set("end", formatTime(req.End))
	values.Set("step", formatStep(req.Step))
	values.Set("table", tableMetadata.TableNameAndType)

	httpReq, err := p.newTimeseriesGetRequest(ctx, "/query_range?"+values.Encode())
	if err != nil {
		return nil, err
	}

	var resp TimeSeriesQueryResponse
	p.newLogger(ctx).Info("Executing timeseries query", "queryString", req.Query)
	if err := p.doRequestAndDecodeResponse(httpReq, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (p *PinotClient) ListTimeSeriesTables(ctx context.Context) ([]string, error) {
	// TODO: Replace with pinot api call when implemented.

	allTables, err := p.ListTables(ctx)
	if err != nil {
		return nil, err
	}

	localCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, len(allTables))
	resultCh := make(chan string, len(allTables))
	defer cancel()
	defer close(errCh)
	defer close(resultCh)

	var wg sync.WaitGroup
	wg.Add(len(allTables))
	for _, table := range allTables {
		go func(table string) {
			schema, err := p.GetTableSchema(localCtx, table)
			if err != nil {
				cancel()
				errCh <- err
			} else if IsTimeSeriesTableSchema(schema) {
				resultCh <- table
			}
			wg.Done()
		}(table)
	}
	wg.Wait()

	var results []string
	for {
		select {
		case table := <-resultCh:
			results = append(results, table)
		case err := <-errCh:
			return nil, err
		default:
			return results, nil
		}
	}
}

type TimeSeriesMetricNamesQuery struct {
	TableName string
	From      time.Time
	To        time.Time
}

func (p *PinotClient) ListTimeSeriesMetrics(ctx context.Context, query TimeSeriesMetricNamesQuery) ([]string, error) {
	// TODO: Replace with pinot api call when implemented.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	sql, err := templates.RenderDistinctValuesSql(templates.DistinctValuesSqlParams{
		ColumnName: TimeSeriesTableColumnMetricName,
		TableName:  query.TableName,
		TimeFilterExpr: TimeFilterExpr(TimeFilter{
			Column: TimeSeriesTableColumnTimestamp,
			Format: DateTimeFormatMillisecondsEpoch(),
			From:   query.From,
			To:     query.To,
		}),
		Limit: templates.DistinctValuesLimit,
	})
	if err != nil {
		return nil, err
	}

	resp, err := p.ExecuteSqlQuery(ctx, SqlQuery{Sql: sql})
	metrics := ExtractStringColumn(resp.ResultTable, 0)
	return metrics, nil
}

type TimeSeriesLabelNamesQuery struct {
	TableName  string
	MetricName string
	From       time.Time
	To         time.Time
}

func (p *PinotClient) ListTimeSeriesLabelNames(ctx context.Context, query TimeSeriesLabelNamesQuery) ([]string, error) {
	// TODO: Replace with pinot api call when implemented.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	collection, err := p.FetchTimeSeriesLabels(ctx, query.TableName, query.MetricName, query.From, query.To)
	if err != nil {
		return nil, err
	}
	return collection.Names(), nil
}

type TimeSeriesLabelValuesQuery struct {
	TableName  string
	MetricName string
	LabelName  string
	From       time.Time
	To         time.Time
}

func (p *PinotClient) ListTimeSeriesLabelValues(ctx context.Context, query TimeSeriesLabelValuesQuery) ([]string, error) {
	// TODO: Replace with pinot api call when implemented.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	collection, err := p.FetchTimeSeriesLabels(ctx, query.TableName, query.MetricName, query.From, query.To)
	if err != nil {
		return nil, err
	}
	return collection.Values(query.LabelName), nil
}

type LabelsCollection map[string]collections.Set[string]

func (x LabelsCollection) Names() []string {
	names := make([]string, 0, len(x))
	for key := range x {
		names = append(names, key)
	}
	sort.Strings(names)
	return names
}

func (x LabelsCollection) Values(name string) []string {
	values := x[name].Values()
	sort.Strings(values)
	return values
}

func (x LabelsCollection) Add(name, value string) {
	if _, ok := x[name]; !ok {
		x[name] = collections.NewSet[string](1)
	}
	x[name].Add(value)
}

func (p *PinotClient) FetchTimeSeriesLabels(ctx context.Context, tableName string, metricName string, from time.Time, to time.Time) (LabelsCollection, error) {
	cacheKey := fmt.Sprintf("table=%s&metric=%s&from=%s&to=%s", tableName, metricName, from.Format(time.RFC3339), to.Format(time.RFC3339))
	return p.timeseriesLabelsCache.Get(cacheKey, func() (LabelsCollection, error) {
		// TODO: This code can be removed once the pinot apis are implemented.

		var filterExprs []string
		if metricName != "" {
			filterExprs = []string{fmt.Sprintf(`"%s" = '%s'`, TimeSeriesTableColumnMetricName, metricName)}
		}

		sql, err := templates.RenderDistinctValuesSql(templates.DistinctValuesSqlParams{
			ColumnName: TimeSeriesTableColumnLabels,
			TableName:  tableName,
			TimeFilterExpr: TimeFilterExpr(TimeFilter{
				Column: TimeSeriesTableColumnTimestamp,
				Format: DateTimeFormatMillisecondsEpoch(),
				From:   from,
				To:     to,
			}),
			DimensionFilterExprs: filterExprs,
			Limit:                templates.DistinctValuesLimit,
		})
		if err != nil {
			return nil, err
		}

		resp, err := p.ExecuteSqlQuery(ctx, SqlQuery{Sql: sql})
		if err != nil {
			return nil, err
		}

		labelRecords, err := ExtractJsonColumn[map[string]string](resp.ResultTable, 0)
		if err != nil {
			return nil, err
		}

		collection := make(LabelsCollection, len(labelRecords))
		for _, label := range labelRecords {
			for k, v := range label {
				collection.Add(k, v)
			}
		}
		return collection, nil
	})
}

func (p *PinotClient) IsTimeSeriesTable(ctx context.Context, tableName string) (bool, error) {
	schema, err := p.GetTableSchema(ctx, tableName)
	if err != nil {
		return false, err
	}

	return IsTimeSeriesTableSchema(schema), nil
}

func IsTimeSeriesTableSchema(schema TableSchema) bool {
	var hasMetricField bool
	for _, fieldSpec := range schema.DimensionFieldSpecs {
		if fieldSpec.Name == TimeSeriesTableColumnMetricName && fieldSpec.DataType == DataTypeString {
			hasMetricField = true
			break
		}
	}
	if !hasMetricField {
		return false
	}

	var hasLabelsField bool
	for _, fieldSpec := range schema.DimensionFieldSpecs {
		if fieldSpec.Name == TimeSeriesTableColumnLabels && fieldSpec.DataType == DataTypeJson {
			hasLabelsField = true
			break
		}
	}
	if !hasLabelsField {
		return false
	}

	var hasValueField bool
	for _, fieldSpec := range schema.MetricFieldSpecs {
		if fieldSpec.Name == TimeSeriesTableColumnMetricValue && fieldSpec.DataType == DataTypeDouble {
			hasValueField = true
			break
		}
	}
	if !hasValueField {
		return false
	}

	var hasTsField bool
	for _, fieldSpec := range schema.DateTimeFieldSpecs {
		if fieldSpec.Name == TimeSeriesTableColumnTimestamp && fieldSpec.DataType == DataTypeTimestamp {
			hasTsField = true
			break
		} else if fieldSpec.Name == TimeSeriesTableColumnTimestamp && fieldSpec.DataType == DataTypeLong {
			hasTsField = true
			break
		}
	}
	if !hasTsField {
		return false
	}

	return true
}

func (p *PinotClient) newTimeseriesGetRequest(ctx context.Context, endpoint string) (*http.Request, error) {
	return p.newRequest(ctx, http.MethodGet, p.properties.BrokerUrl+TimeSeriesEndpoint+endpoint, nil)
}
