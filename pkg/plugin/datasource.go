package plugin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
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

func createPinotClient(pinotConfigMap map[string]interface{}, pinotSecureConfigMap map[string]string) (*pinot.Connection, error) {
	var controllerUrl = pinotConfigMap["controllerUrl"].(string)
	var brokerUrl = pinotConfigMap["brokerUrl"].(string)

	var authToken, authTokenPresent = pinotSecureConfigMap["authToken"]
	var headers = map[string]string{}
	if authTokenPresent {
		headers["Authorization"] = authToken
	}
	var clientConfig *pinot.ClientConfig

	if brokerUrl != "" {
		clientConfig = &pinot.ClientConfig{
			BrokerList:      []string{brokerUrl},
			ExtraHTTPHeader: headers,
		}
	} else if controllerUrl != "" {
		clientConfig = &pinot.ClientConfig{
			ControllerConfig: &pinot.ControllerConfig{
				ControllerAddress: controllerUrl,
			},
			ExtraHTTPHeader: headers,
		}
	} else {
		return nil, fmt.Errorf("no valid Pinot connection configs found")
	}
	backend.Logger.Info("Connecting to Pinot with config: %v", clientConfig)
	return pinot.NewWithConfig(clientConfig)
}

// NewDatasource creates a new datasource instance.
func NewDatasource(settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {

	var pinotConfigMap map[string]interface{}
	if settings.JSONData != nil {
		if err := json.Unmarshal(settings.JSONData, &pinotConfigMap); err != nil {
			return nil, err
		}
	}
	// Create Pinot Client, get brokers from the settings
	client, err := createPinotClient(pinotConfigMap, settings.DecryptedSecureJSONData)
	return &Datasource{
		client:        *client,
		controllerUrl: pinotConfigMap["controllerUrl"].(string),
	}, err
}

// Datasource is an example datasource which can respond to data queries, reports
// its health and has streaming skills.
type Datasource struct {
	client        pinot.Connection
	controllerUrl string
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
	RawSql       string  `json:"rawSql"`
	QueryType    string  `json:"editorType"`
	TableName    string  `json:"tableName"`
	Fill         bool    `json:"fill"`
	FillInterval float64 `json:"fillInterval"`
	FillMode     string  `json:"fillMode"`
	FillValue    float64 `json:"fillValue"`
	Format       string  `json:"format"`
}

func (d *Datasource) query(context context.Context, pCtx backend.PluginContext, query backend.DataQuery) backend.DataResponse {
	var response backend.DataResponse

	// Unmarshal the JSON into our queryModel.
	var queryModel queryModel

	err := json.Unmarshal(query.JSON, &queryModel)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("json unmarshal: %v", err.Error()))
	}

	backend.Logger.Info("json unmarshal: %v", queryModel)
	from := query.TimeRange.From.UnixMilli()
	to := query.TimeRange.To.UnixMilli()
	interval := query.Interval.Milliseconds()
	parser := CreateParser(queryModel.RawSql, queryModel.QueryType)
	table := queryModel.TableName
	queryRepresentation, _ := parser.parse()

	backend.Logger.Info(fmt.Sprintf("Parsed query : %v", queryRepresentation))
	sqlQuery := queryRepresentation.toSqlQuery(table, interval, from, to)
	backend.Logger.Info(fmt.Sprintf("Running query : %s", sqlQuery))
	resp, err := d.client.ExecuteSQL(table, sqlQuery)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("json unmarshal: %v", err.Error()))
	}

	// create data frame response.
	// For an overview on data frames and how grafana handles them:
	// https://grafana.com/docs/grafana/latest/developers/plugins/data-frames/
	frame := queryRepresentation.extractResults(resp.ResultTable) // extractResults(resp.ResultTable)

	// add the frames to the response.
	response.Frames = append(response.Frames, frame)

	return response
}

// CheckHealth handles health checks sent from Grafana to the plugin.
// The main use case for these health checks is the test button on the
// datasource configuration page which allows users to verify that
// a datasource is working as expected.
func (d *Datasource) CheckHealth(_ context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	var status = backend.HealthStatusOk
	var message = "Pinot data source is working"
	_, err := d.client.ExecuteSQL("", "select 1")
	if err != nil {
		status = backend.HealthStatusError
		message = err.Error()
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
