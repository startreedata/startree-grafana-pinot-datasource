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

func (query SqlQuery) cacheKey() string {
	return fmt.Sprintf("%v", query)
}

// RenderSql returns the SQL that ExecuteSqlQuery sends: the query's SQL with the client's row limit applied.
func (p *Client) RenderSql(query SqlQuery) string {
	return applyRowLimit(query.Sql, p.properties.MaxRowLimit)
}

// mergeQueryOptions returns options followed by the client's options. For each option it:
//   - trims the name and the value.
//   - removes one pair of single quotes around the value. Users quoted string values because the SET
//     syntax needed them, and Pinot's SET parser removed the quotes.
//   - drops the option if the name or the value is empty.
//
// On a duplicate name (case-insensitive), the last option wins. So a datasource option overrides a query option
// with the same name.
func (p *Client) mergeQueryOptions(options []QueryOption) []QueryOption {
	var merged []QueryOption
	indexByName := make(map[string]int)
	for _, o := range slices.Concat(options, p.properties.QueryOptions) {
		name := strings.TrimSpace(o.Name)
		value := strings.TrimSpace(o.Value)
		if len(value) >= 2 && strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
			value = value[1 : len(value)-1]
		}
		if name == "" || value == "" {
			continue
		}

		key := strings.ToLower(name)
		if i, ok := indexByName[key]; ok {
			merged[i] = QueryOption{Name: name, Value: value}
		} else {
			indexByName[key] = len(merged)
			merged = append(merged, QueryOption{Name: name, Value: value})
		}
	}
	return merged
}

// renderQueryOptions formats options for the broker request's queryOptions field, e.g. "k1=v1;k2=v2".
func renderQueryOptions(options []QueryOption) string {
	pairs := make([]string, len(options))
	for i, o := range options {
		pairs[i] = o.Name + "=" + o.Value
	}
	return strings.Join(pairs, ";")
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

// sqlRequest is the body of a broker /query/sql request.
type sqlRequest struct {
	Sql          string `json:"sql"`
	QueryOptions string `json:"queryOptions,omitempty"`
	Trace        bool   `json:"trace,omitempty"`
}

func (p *Client) sqlRequestOf(query SqlQuery) sqlRequest {
	return sqlRequest{
		Sql:          p.RenderSql(query),
		QueryOptions: renderQueryOptions(p.mergeQueryOptions(query.QueryOptions)),
		Trace:        query.Trace,
	}
}

func (p *Client) ExecuteSqlQuery(ctx context.Context, query SqlQuery) (*BrokerResponse, error) {
	if p.properties.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.properties.QueryTimeout)
		defer cancel()
	}

	request := p.sqlRequestOf(query)
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(request); err != nil {
		return nil, err
	}

	req, err := p.newBrokerPostRequest(ctx, "/query/sql", &body)
	if err != nil {
		return nil, err
	}

	p.logger.Info("pinot/http: Executing sql query.", "queryString", request.Sql, "queryOptions", request.QueryOptions)

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
