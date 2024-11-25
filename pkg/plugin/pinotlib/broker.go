package pinotlib

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type BrokerResponse struct {
	ResultTable *ResultTable      `json:"resultTable,omitempty"`
	TraceInfo   map[string]string `json:"traceInfo,omitempty"`
	Exceptions  []BrokerException `json:"exceptions"`

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

func (x *BrokerResponse) HasExceptions() bool {
	return len(x.Exceptions) > 0
}

func (x *BrokerResponse) HasData() bool {
	return x.ResultTable != nil && x.ResultTable.RowCount() > 0
}

type BrokerException struct {
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

func (x *ResultTable) RowCount() int {
	return len(x.Rows)
}

func (x *ResultTable) ColumnCount() int {
	return len(x.DataSchema.ColumnNames)
}

type BrokerExceptionError struct {
	Exceptions []BrokerException
}

func NewBrokerExceptionError(exceptions []BrokerException) *BrokerExceptionError {
	return &BrokerExceptionError{exceptions}
}

func (e *BrokerExceptionError) Error() string {
	messages := make([]string, len(e.Exceptions))
	for i, exception := range e.Exceptions {
		messages[i] = fmt.Sprintf("Code %d: %s", exception.ErrorCode, exception.Message)
	}
	return "Broker request completed with exceptions:\n" + strings.Join(messages, "\n")
}

type SqlQuery struct {
	Sql          string
	Trace        bool
	QueryOptions map[string]string
}

func NewSqlQuery(sql string) SqlQuery {
	return SqlQuery{Sql: sql}
}

var queryLock sync.Mutex

func (p *PinotClient) ExecuteSqlQuery(ctx context.Context, query SqlQuery) (*BrokerResponse, error) {
	queryLock.Lock()
	defer queryLock.Unlock()
	time.Sleep(1 * time.Second)
	defer func() {
		time.Sleep(1 * time.Second)
	}()

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

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(data); err != nil {
		return nil, err
	}

	req, err := p.newBrokerPostRequest(ctx, "/query/sql", &body)
	if err != nil {
		return nil, err
	}

	p.newLogger(ctx).Info("Executing sql query.", "queryString", query.Sql)

	var respData BrokerResponse
	resp, err := p.doRequest(req)
	if err != nil {
		return nil, err
	}
	defer p.closeResponseBody(ctx, resp)

	if resp.StatusCode != http.StatusOK {
		return nil, p.newErrorFromResponseBody(ctx, resp)
	}

	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	if err = decoder.Decode(&respData); err != nil {
		return nil, fmt.Errorf("pinot/http: failed to decode response json: %w", err)
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
