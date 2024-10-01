package pinotlib

import (
	"context"
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/collections"
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

func (p *PinotClient) ExecuteTimeSeriesQuery(ctx context.Context, req *TimeSeriesRangeQuery) (*http.Response, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

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

	return p.doRequest(httpReq)
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

	timeExprBuilder, err := NewTimeExpressionBuilder(TimeSeriesTableColumnTimestamp, TimeSeriesTimestampFormat)
	if err != nil {
		return nil, err
	}

	sql, err := templates.RenderDistinctValuesSql(templates.DistinctValuesSqlParams{
		ColumnName:     TimeSeriesTableColumnMetricName,
		TableName:      query.TableName,
		TimeFilterExpr: timeExprBuilder.TimeFilterExpr(query.From, query.To),
		Limit:          templates.DistinctValuesLimit,
	})
	if err != nil {
		return nil, err
	}

	resp, err := p.ExecuteSQL(ctx, query.TableName, sql)
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

		timeExprBuilder, err := NewTimeExpressionBuilder(TimeSeriesTableColumnTimestamp, TimeSeriesTimestampFormat)
		if err != nil {
			return nil, err
		}

		var filterExprs []string
		if metricName != "" {
			filterExprs = []string{fmt.Sprintf(`"%s" = '%s'`, TimeSeriesTableColumnMetricName, metricName)}
		}

		sql, err := templates.RenderDistinctValuesSql(templates.DistinctValuesSqlParams{
			ColumnName:           TimeSeriesTableColumnLabels,
			TableName:            tableName,
			TimeFilterExpr:       timeExprBuilder.TimeFilterExpr(from, to),
			DimensionFilterExprs: filterExprs,
			Limit:                templates.DistinctValuesLimit,
		})
		if err != nil {
			return nil, err
		}

		resp, err := p.ExecuteSQL(ctx, tableName, sql)
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
