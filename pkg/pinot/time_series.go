package pinot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

const (
	TimeSeriesEndpoint = "/timeseries/api/v1"

	TimeSeriesTableColumnMetricName  = "metric"
	TimeSeriesTableColumnLabels      = "labels"
	TimeSeriesTableColumnMetricValue = "value"
	TimeSeriesTableColumnTimestamp   = "ts"
	TimeSeriesQueryLanguagePromQl    = "promql"
)

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
	if err := json.Unmarshal([]byte(metric["labels"]), &labels); err != nil {
		return metric
	}

	labels["__name__"] = metric["__name__"]
	return labels
}

func (p *Client) IsTimeseriesSupported(ctx context.Context) (bool, error) {
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

func (p *Client) ExecuteTimeSeriesQuery(ctx context.Context, req *TimeSeriesRangeQuery) (*TimeSeriesQueryResponse, error) {
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
	p.logger.Info("pinot/http: Executing timeseries query.", "queryString", req.Query)
	if err := p.doRequestAndDecodeResponse(httpReq, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (p *Client) ListTimeSeriesTables(ctx context.Context) ([]string, error) {
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

func (p *Client) ListTimeSeriesMetrics(ctx context.Context, query TimeSeriesMetricNamesQuery) ([]string, error) {
	// TODO: Replace with pinot api call when implemented.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	sql, err := RenderDistinctValuesSql(DistinctValuesSqlParams{
		ColumnExpr: ObjectExpr(TimeSeriesTableColumnMetricName),
		TableName:  query.TableName,
		TimeFilterExpr: TimeFilterExpr(TimeFilter{
			Column: TimeSeriesTableColumnTimestamp,
			Format: DateTimeFormatMillisecondsEpoch(),
			From:   query.From,
			To:     query.To,
		}),
	})
	if err != nil {
		return nil, err
	}

	resp, err := p.ExecuteSqlQuery(ctx, SqlQuery{Sql: sql})
	switch {
	case err != nil:
		return nil, err
	case resp.HasData():
		return ExtractColumnAsStrings(resp.ResultTable, 0)
	default:
		return nil, nil
	}
}

type TimeSeriesLabelNamesQuery struct {
	TableName  string
	MetricName string
	From       time.Time
	To         time.Time
}

func (p *Client) ListTimeSeriesLabelNames(ctx context.Context, query TimeSeriesLabelNamesQuery) ([]string, error) {
	// TODO: Replace with pinot api call when implemented.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	collection, err := p.fetchTimeSeriesLabels(ctx, query.TableName, query.MetricName, query.From, query.To)
	if err != nil {
		return nil, err
	}
	return collection.names(), nil
}

type TimeSeriesLabelValuesQuery struct {
	TableName  string
	MetricName string
	LabelName  string
	From       time.Time
	To         time.Time
}

func (p *Client) ListTimeSeriesLabelValues(ctx context.Context, query TimeSeriesLabelValuesQuery) ([]string, error) {
	// TODO: Replace with pinot api call when implemented.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	collection, err := p.fetchTimeSeriesLabels(ctx, query.TableName, query.MetricName, query.From, query.To)
	if err != nil {
		return nil, err
	}
	return collection.valuesOf(query.LabelName), nil
}

type labelsCollection struct {
	labelNames  []string
	nameToLabel map[string]label
}

type label struct {
	name     string
	values   []string
	valueSet map[string]struct{}
}

func (x *labelsCollection) names() []string {
	return x.labelNames
}

func (x *labelsCollection) valuesOf(name string) []string {
	return x.nameToLabel[name].values
}

func (x *labelsCollection) add(name, value string) {
	if x.nameToLabel == nil {
		x.nameToLabel = make(map[string]label)
	}

	if _, ok := x.nameToLabel[name]; !ok {
		x.labelNames = append(x.labelNames, name)
		x.nameToLabel[name] = label{
			name:     name,
			valueSet: make(map[string]struct{}),
		}
	}
	thisLabel := x.nameToLabel[name]

	if _, ok := thisLabel.valueSet[value]; !ok {
		thisLabel.valueSet[value] = struct{}{}
		thisLabel.values = append(thisLabel.values, value)
		x.nameToLabel[name] = thisLabel
	}
}

func (p *Client) fetchTimeSeriesLabels(ctx context.Context, tableName string, metricName string, from time.Time, to time.Time) (*labelsCollection, error) {
	var filterExprs []SqlExpr
	if metricName != "" {
		filterExprs = []SqlExpr{SqlExpr(fmt.Sprintf(`"%s" = '%s'`, TimeSeriesTableColumnMetricName, metricName))}
	}

	dataType, err := p.timeSeriesLabelType(ctx, tableName)
	if err != nil {
		return nil, err
	}

	var columnExpr SqlExpr
	if dataType == DataTypeJson {
		columnExpr = ObjectExpr(TimeSeriesTableColumnLabels)
	} else {
		columnExpr = CastExpr(ObjectExpr(TimeSeriesTableColumnLabels), DataTypeJson)
	}

	sql, err := RenderDistinctValuesSql(DistinctValuesSqlParams{
		ColumnExpr: columnExpr,
		TableName:  tableName,
		TimeFilterExpr: TimeFilterExpr(TimeFilter{
			Column: TimeSeriesTableColumnTimestamp,
			Format: DateTimeFormatMillisecondsEpoch(),
			From:   from,
			To:     to,
		}),
		DimensionFilterExprs: filterExprs,
	})
	if err != nil {
		return nil, err
	}

	resp, err := p.ExecuteSqlQuery(ctx, SqlQuery{Sql: sql})
	switch {
	case err != nil:
		return nil, err
	case resp.HasData():
		return extractLabels(resp.ResultTable)
	default:
		return nil, nil
	}
}

func (p *Client) timeSeriesLabelType(ctx context.Context, tableName string) (string, error) {
	schema, err := p.GetTableSchema(ctx, tableName)
	if err != nil {
		return "", err
	}

	for _, x := range schema.DimensionFieldSpecs {
		if x.Name == TimeSeriesTableColumnLabels {
			return x.DataType, nil
		}
	}
	for _, x := range schema.ComplexFieldSpecs {
		if x.Name == TimeSeriesTableColumnLabels {
			return x.DataType, nil
		}
	}
	return "", fmt.Errorf("not a time series table")
}

func extractLabels(results *ResultTable) (*labelsCollection, error) {
	labelRecords, err := DecodeJsonFromColumn[map[string]string](results, 0)
	if err != nil {
		return nil, err
	}

	var collection labelsCollection
	for _, record := range labelRecords {
		for k, v := range record {
			collection.add(k, v)
		}
	}
	return &collection, nil
}

func (p *Client) IsTimeSeriesTable(ctx context.Context, tableName string) (bool, error) {
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
	for _, fieldSpec := range schema.ComplexFieldSpecs {
		if fieldSpec.Name == TimeSeriesTableColumnLabels && fieldSpec.DataType == DataTypeMap {
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

func (p *Client) newTimeseriesGetRequest(ctx context.Context, endpoint string) (*http.Request, error) {
	return p.newRequest(ctx, http.MethodGet, p.properties.BrokerUrl+TimeSeriesEndpoint+endpoint, nil)
}
