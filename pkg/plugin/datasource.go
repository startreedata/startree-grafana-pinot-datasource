package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	pinot "github.com/startreedata/pinot-client-go/pinot"
)

// Make sure Datasource implements required interfaces. This is important to do
// since otherwise we will only get a not implemented error response from plugin in
// runtime. In this example datasource instance implements backend.QueryDataHandler,
// backend.CheckHealthHandler interfaces. Plugin should not implement all these
// interfaces- only those which are required for a particular task.
var (
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

// NewDatasource creates a new datasource instance.
func NewDatasource(_ backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	// Create Pinot Client
	client, err := pinot.NewFromController("http://localhost:9000")

	return &Datasource{
		client: *client,
	}, err
}

// Datasource is an example datasource which can respond to data queries, reports
// its health and has streaming skills.
type Datasource struct {
	client pinot.Connection
}

// Dispose here tells plugin SDK that plugin wants to clean up resources when a new instance
// created. As soon as datasource settings change detected by SDK old datasource instance will
// be disposed and a new one will be created using NewSampleDatasource factory function.
func (d *Datasource) Dispose() {
	// Clean up datasource instance resources.
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	// create response struct
	response := backend.NewQueryDataResponse()

	// loop over queries and execute them individually.
	for _, q := range req.Queries {
		println(q.JSON)
		res := d.query(ctx, req.PluginContext, q)

		// save the response in a hashmap
		// based on with RefID as identifier
		response.Responses[q.RefID] = res
	}

	return response, nil
}

type queryModel struct {
	QueryText    string  `json:"queryText"`
	Fill         bool    `json:"fill"`
	FillInterval float64 `json:"fillInterval"`
	FillMode     string  `json:"fillMode"`
	FillValue    float64 `json:"fillValue"`
	Format       string  `json:"format"`
}

func (d *Datasource) query(_ context.Context, pCtx backend.PluginContext, query backend.DataQuery) backend.DataResponse {
	var response backend.DataResponse

	// Unmarshal the JSON into our queryModel.
	var qm queryModel

	err := json.Unmarshal(query.JSON, &qm)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("json unmarshal: %v", err.Error()))
	}

	from := query.TimeRange.From.UnixMilli()
	to := query.TimeRange.To.UnixMilli()
	interval := query.Interval.Milliseconds()
	parser := CreateParser(qm.QueryText)
	// try just metric
	sqlQuery := ""
	if agg, good := parser.ParseAggregation(); good {
		sqlQuery = AggToSql("metrics_hc_sort_time", interval, agg, from, to)
	} else if metric, good := parser.parseMetric(); good {
		sqlQuery = MetricToSql("metrics_hc_sort_time", interval, metric, from, to)
	} else if logQl, good := parser.parseLogQl(); good {
		sqlQuery = LogQlToSql("logQlTable", interval, logQl, from, to)
	}

	resp, err := d.client.ExecuteSQL("metrics_hc_sort_time", sqlQuery)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("json unmarshal: %v", err.Error()))
	}

	// create data frame response.
	// For an overview on data frames and how grafana handles them:
	// https://grafana.com/docs/grafana/latest/developers/plugins/data-frames/
	frame := extractResults(resp.ResultTable)

	// add the frames to the response.
	response.Frames = append(response.Frames, frame)

	return response
}

func extractResults(results *pinot.ResultTable) *data.Frame {
	// Get the time columna
	timeIdx, _ := getColumnIdx("time", &results.DataSchema)
	valueIdx, _ := getColumnIdx("value", &results.DataSchema)

	times := []time.Time{}
	values := []float64{}

	// Iterate over each row
	for rowIdx := 0; rowIdx < results.GetRowCount(); rowIdx++ {
		// Extract timestamp
		ts := int64(results.GetDouble(rowIdx, timeIdx))
		times = append(times, time.UnixMilli(ts))

		// Extract labels
		// Extract value
		value := results.GetDouble(rowIdx, valueIdx)
		values = append(values, value)
	}

	// create data frame response.
	// For an overview on data frames and how grafana handles them:
	// https://grafana.com/docs/grafana/latest/developers/plugins/data-frames/
	frame := data.NewFrame("response")

	// add fields.
	frame.Fields = append(frame.Fields,
		data.NewField("time", nil, times),
		data.NewField("values", nil, values),
	)

	// add the frames to the response.
	return frame
}

func getColumnIdx(col string, schema *pinot.RespSchema) (int, error) {
	for idx := 0; idx < len(schema.ColumnNames); idx++ {
		if schema.ColumnNames[idx] == col {
			return idx, nil
		}
	}

	return -1, fmt.Errorf("Column not found")
}

// CheckHealth handles health checks sent from Grafana to the plugin.
// The main use case for these health checks is the test button on the
// datasource configuration page which allows users to verify that
// a datasource is working as expected.
func (d *Datasource) CheckHealth(_ context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	var status = backend.HealthStatusOk
	var message = "Data source is working"

	if rand.Int()%2 == 0 {
		status = backend.HealthStatusError
		message = "randomized error"
	}

	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}

/*
Parse PromQL:
- id: [A-Za-z_0-9]
- string: ".*"
- metric: <id>[{<id> : <string>}]
- by: by(<id>[, <id>]*)
- aggregate: <id> [<by>] (<metric>)   (start with sum and avg)


type:
	Aggregate
		- operator name
		- option BY clause
		- metric

	By:
		labels: []string

	Metric:
		name: str
		label filters: []LabelFilter

	LabelFilter
		name: str
		value: str
*/
