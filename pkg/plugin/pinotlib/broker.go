package pinotlib

import (
	"bytes"
	"context"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/logger"
	"io"
	"net/http"
	"sort"
	"strings"
)

type BrokerResponse struct {
	ResultTable *ResultTable      `json:"resultTable,omitempty"`
	TraceInfo   map[string]string `json:"traceInfo,omitempty"`
	Exceptions  []PinotException  `json:"exceptions"`

	NumSegmentsProcessed        int64 `json:"numSegmentsProcessed"`
	NumServersResponded         int64 `json:"numServersResponded"`
	NumSegmentsQueried          int64 `json:"numSegmentsQueried"`
	NumServersQueried           int64 `json:"numServersQueried"`
	NumSegmentsMatched          int64 `json:"numSegmentsMatched"`
	NumConsumingSegmentsQueried int64 `json:"numConsumingSegmentsQueried"`
	NumDocsScanned              int64 `json:"numDocsScanned"`
	NumEntriesScannedInFilter   int64 `json:"numEntriesScannedInFilter"`
	NumEntriesScannedPostFilter int64 `json:"numEntriesScannedPostFilter"`
	TotalDocs                   int64 `json:"totalDocs"`
	TimeUsedMs                  int64 `json:"timeUsedMs"`
	MinConsumingFreshnessTimeMs int64 `json:"minConsumingFreshnessTimeMs"`
}

type PinotException struct {
	Message   string `json:"message"`
	ErrorCode int    `json:"errorCode"`
}

type DataSchema struct {
	ColumnDataTypes []string `json:"columnDataTypes"`
	ColumnNames     []string `json:"columnNames"`
}

type ResultTable struct {
	DataSchema DataSchema      `json:"dataSchema"`
	Rows       [][]interface{} `json:"rows"`
}

type BrokerExceptionError struct {
	Exceptions []PinotException
}

func (e *BrokerExceptionError) Error() string {
	if len(e.Exceptions) > 0 {
		return e.Exceptions[0].Message
	}
	return "nondescript broker exception"
}

type SqlQuery struct {
	Sql          string
	Trace        bool
	QueryOptions map[string]string
}

func NewSqlQuery(sql string) SqlQuery {
	return SqlQuery{Sql: sql}
}

func (p *PinotClient) ExecuteSqlQuery(ctx context.Context, query SqlQuery) (*BrokerResponse, error) {
	var body bytes.Buffer

	data := map[string]interface{}{"sql": query.Sql}
	if query.Trace {
		data["trace"] = true
	}
	if len(query.QueryOptions) > 0 {
		var queryOptionsEncoded []string
		for key, value := range query.QueryOptions {
			queryOptionsEncoded = append(queryOptionsEncoded, key+"="+value)
		}
		sort.Strings(queryOptionsEncoded)
		data["queryOptions"] = strings.Join(queryOptionsEncoded, ";")
	}

	err := json.NewEncoder(&body).Encode(data)
	if err != nil {
		return nil, err
	}

	req, err := p.newBrokerPostRequest(ctx, "/query/sql", &body)
	if err != nil {
		return nil, err
	}

	logger.Logger.Info(fmt.Sprintf("pinot/http: executing sql query: %s", query.Sql))

	var respData BrokerResponse
	resp, err := p.doRequest(req)
	if err != nil {
		return nil, err
	}
	defer p.closeResponseBody(resp)

	if resp.StatusCode != http.StatusOK {
		return nil, p.newErrorFromResponseBody(resp)
	}

	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	if err = decoder.Decode(&respData); err != nil {
		return nil, fmt.Errorf("pinot/http failed to decode response json: %w", err)
	}

	if len(respData.Exceptions) > 0 {
		return nil, &BrokerExceptionError{respData.Exceptions}
	}
	return &respData, nil
}

func (p *PinotClient) newBrokerPostRequest(ctx context.Context, endpoint string, body io.Reader) (*http.Request, error) {
	req, err := p.newRequest(ctx, http.MethodPost, p.properties.BrokerUrl+endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	return req, nil
}
