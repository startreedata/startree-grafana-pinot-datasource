package pinot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
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
	QueryOptions []QueryOption
}

func NewSqlQuery(sql string) SqlQuery {
	return SqlQuery{Sql: sql}
}

// RenderSql returns the SQL query string with all query options appended.
func (query SqlQuery) RenderSql() string {
	sql := strings.TrimSpace(query.Sql)
	if len(query.QueryOptions) == 0 {
		return sql
	}

	var builder strings.Builder
	builder.WriteString(sql)
	if strings.HasSuffix(sql, ";") {
		builder.WriteString("\n")
	} else {
		builder.WriteString(";\n")
	}
	for _, o := range query.QueryOptions {
		builder.WriteString(fmt.Sprintf("\nSET %s=%s;", o.Name, o.Value))
	}
	return builder.String()
}

func (query SqlQuery) cacheKey() string {
	return fmt.Sprintf("%v", query)
}

// RenderSql renders the actual SQL query string sent to Pinot.
// The rendered query includes all query options provided in the client properties and query.
func (p *Client) RenderSql(query SqlQuery) string {
	query.Sql = applyRowLimit(query.Sql, p.properties.MaxRowLimit)
	query.QueryOptions = slices.Concat(query.QueryOptions, p.properties.QueryOptions)
	return query.RenderSql()
}

var limitClauseRe = regexp.MustCompile(`(?i)\blimit\s+\d`)

// applyRowLimit appends a LIMIT clause to cap result size when the max-row guardrail is
// configured and the query has no explicit LIMIT. Protects the broker from unbounded scans.
// ponytail: regex LIMIT detection — a string literal/identifier like 'limit 1' would
// false-positive and skip the cap. Upgrade to a real SQL parser only if that ever bites.
func applyRowLimit(sql string, maxRowLimit int) string {
	if maxRowLimit <= 0 || limitClauseRe.MatchString(sql) {
		return sql
	}
	trimmed := strings.TrimRight(strings.TrimSpace(sql), "; \t\r\n")
	return fmt.Sprintf("%s LIMIT %d", trimmed, maxRowLimit)
}

func (p *Client) ExecuteSqlQuery(ctx context.Context, query SqlQuery) (*BrokerResponse, error) {
	if p.properties.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.properties.QueryTimeout)
		defer cancel()
	}

	request := struct {
		Sql   string `json:"sql"`
		Trace bool   `json:"trace,omitempty"`
	}{
		Sql:   p.RenderSql(query),
		Trace: query.Trace,
	}

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(request); err != nil {
		return nil, err
	}

	req, err := p.newBrokerPostRequest(ctx, "/query/sql", &body)
	if err != nil {
		return nil, err
	}

	p.logger.Info("pinot/http: Executing sql query.", "queryString", request.Sql)

	var respData BrokerResponse
	if err = p.doRequestAndDecodeResponse(req, &respData); err != nil {
		return nil, err
	}
	return &respData, nil
}

func (p *Client) newBrokerPostRequest(ctx context.Context, endpoint string, body io.Reader) (*http.Request, error) {
	req, err := p.newRequest(ctx, http.MethodPost, p.properties.BrokerUrl+endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	return req, nil
}
