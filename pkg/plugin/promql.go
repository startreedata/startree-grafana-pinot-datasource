package plugin

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	pinot "github.com/startreedata/pinot-client-go/pinot"
)

func filterToWhereClause(metricName string, interval, from, to int64, labels []LabelFilter) string {
	if len(labels) == 0 {
		return fmt.Sprintf(`WHERE name='%s' AND bucket >= %d AND bucket <= %d`, metricName, from/interval, to/interval)
	} else {
		return fmt.Sprintf(`WHERE name='%s' AND labels='%s:%s' AND bucket >= %d AND bucket <= %d`, metricName, labels[0].Label, labels[0].Value, from/interval, to/interval)
	}
}

type LabelFilter struct {
	Label string
	Value string
	Op    string
}

func (l *LabelFilter) String() string {
	switch l.Op {
	case "=":
		return l.Label + "='" + l.Value + "'"
	case "!=":
		return l.Label + "!='" + l.Value + "'"
	case "=~":
		return "REGEXP_LIKE(" + l.Label + ", '" + l.Value + "')"
	case "!~":
		return "not(REGEXP_LIKE(" + l.Label + ", '" + l.Value + "'))"
	case "|=":
		return "REGEXP_LIKE(" + l.Label + ", '" + l.Value + "')"
	default:
		return ""
	}
}

type QueryRepresentation interface {
	toSqlQuery(table string, interval, from, to int64) string
	extractResults(results *pinot.ResultTable) *data.Frame
}

type Metric struct {
	Name         string
	LabelFilters []LabelFilter
}

func (m Metric) extractResults(results *pinot.ResultTable) *data.Frame {
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

func (metric Metric) toSqlQuery(table string, interval, from, to int64) string {
	return fmt.Sprintf(
		`SELECT min("time") as "time", avg(value) as value, floor("time" / %d) as bucket 
			 FROM %s 
			 %s
			 GROUP BY bucket 
			 ORDER BY bucket ASC
			 LIMIT 1000`,
		interval, table, filterToWhereClause(metric.Name, interval, from, to, metric.LabelFilters))
}

type LogQlQuery struct {
	labelFilters []LabelFilter
	logFilters   []LabelFilter
}

func (q LogQlQuery) toSqlQuery(table string, interval, from, to int64) string {
	return fmt.Sprintf("SELECT logLine as value, timestampInEpoch as tstamp FROM %s %s ORDER BY timestampInEpoch ASC LIMIT 1000",
		table, q.LogQlToWhereClause(interval, from, to))
}

func (q LogQlQuery) LogQlToWhereClause(interval, from, to int64) string {
	var whereClause string
	whereClause = fmt.Sprintf("WHERE timestampInEpoch >= %d AND timestampInEpoch <= %d", from, to)

	for i := 0; i < len(q.labelFilters); i++ {
		whereClause += " AND " + q.labelFilters[i].String()
	}

	for i := 0; i < len(q.logFilters); i++ {
		whereClause += " AND " + q.logFilters[i].String()
	}

	return whereClause
}

func (q LogQlQuery) extractResults(results *pinot.ResultTable) *data.Frame {
	// Get the time columna
	timeIdx, _ := getColumnIdx("tstamp", &results.DataSchema)
	valueIdx, _ := getColumnIdx("value", &results.DataSchema)

	times := []time.Time{}
	values := []string{}

	// Iterate over each row
	for rowIdx := 0; rowIdx < results.GetRowCount(); rowIdx++ {
		// Extract timestamp
		ts := int64(results.GetDouble(rowIdx, timeIdx))
		times = append(times, time.UnixMilli(ts))
		value := results.GetString(rowIdx, valueIdx)
		values = append(values, value)
	}

	// create data frame response.
	// For an overview on data frames and how grafana handles them:
	// https://grafana.com/docs/grafana/latest/developers/plugins/data-frames/
	frame := data.NewFrame("response")

	// add fields.
	frame.Fields = append(frame.Fields,
		data.NewField("timestamp", nil, times),
		data.NewField("body", nil, values),
	)

	// add the frames to the response.
	return frame
}

type Aggregation struct {
	Op     string
	Metric Metric
	By     By
}

func (agg Aggregation) toSqlQuery(table string, interval, from, to int64) string {
	sqlAgg := ""
	if strings.ToLower(agg.Op) == "avg" {
		sqlAgg = "avg"
	} else if strings.ToLower(agg.Op) == "sum" {
		sqlAgg = "sum"
	}

	return fmt.Sprintf(
		`SELECT min("time") as "time", %s(value) as value, floor("time" / %d) as bucket 
			 FROM %s 
			 %s
			 GROUP BY bucket 
			 ORDER BY bucket ASC
			 LIMIT 1000`,
		sqlAgg, interval, table, filterToWhereClause(agg.Metric.Name, interval, from, to, agg.Metric.LabelFilters))
}

func (agg Aggregation) extractResults(results *pinot.ResultTable) *data.Frame {
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

type By struct {
	Labels []string
}

type Parser struct {
	idx    int
	stream []rune
}

func CreateParser(text string) Parser {
	return Parser{
		idx:    0,
		stream: []rune(text),
	}
}

func ParsePromQL() {

}

func (p *Parser) parseID() (string, bool) {
	// Skip leading white space
	for p.idx < len(p.stream) && unicode.IsSpace(p.stream[p.idx]) {
		p.idx += 1
	}

	if p.idx == len(p.stream) {
		return "", false
	}

	start := p.idx

	// Check that first char is not a digit
	if unicode.IsDigit(p.stream[p.idx]) || p.stream[p.idx] == '_' {
		return "", false
	}

	// read until whitespace or non-alphanumeric
	for p.idx < len(p.stream) && (unicode.IsLetter(p.stream[p.idx]) || unicode.IsDigit(p.stream[p.idx]) || p.stream[p.idx] == '_') {
		p.idx += 1
	}
	end := p.idx

	if start == end {
		return "", false
	}

	// Convert slice into string
	id := string(p.stream[start:end])

	// Update the cursor and return the string
	return id, true
}

func (p *Parser) parseString() (string, bool) {
	// Skip leading white space
	for p.idx < len(p.stream) && unicode.IsSpace(p.stream[p.idx]) {
		p.idx += 1
	}

	if p.idx == len(p.stream) {
		return "", false
	}

	// First char is \"
	if p.stream[p.idx] != '"' {
		return "", false
	}
	p.idx += 1
	start := p.idx
	// Read until " is found
	for p.idx < len(p.stream) && p.stream[p.idx] != '"' {
		p.idx += 1
	}
	// If EOF then return false
	if p.idx == len(p.stream) || p.stream[p.idx] != '"' {
		return "", false
	}

	end := p.idx
	p.idx += 1

	return string(p.stream[start:end]), true
}

func (p *Parser) parseLabelFilter() (LabelFilter, bool) {
	tmpIdx := p.idx
	// Skip leading white space
	for p.idx < len(p.stream) && unicode.IsSpace(p.stream[p.idx]) {
		p.idx += 1
	}

	if p.idx == len(p.stream) {
		p.idx = tmpIdx
		return LabelFilter{}, false
	}

	// Read ID
	label, good := p.parseID()
	if !good {
		p.idx = tmpIdx
		return LabelFilter{}, false
	}
	// Read =
	if !p.parseChar('=') {
		p.idx = tmpIdx
		return LabelFilter{}, false
	}
	// Read string
	value, good := p.parseString()
	if !good {
		p.idx = tmpIdx
		return LabelFilter{}, false
	}

	return LabelFilter{Label: label, Value: value}, true
}

func (p *Parser) parseMetric() (Metric, bool) {
	if !p.skipWhitespace() {
		return Metric{}, false
	}

	tmpIdx := p.idx
	// Read ID
	name, good := p.parseID()
	if !good {
		p.idx = tmpIdx
		return Metric{}, false
	}

	if !p.skipWhitespace() {
		return Metric{Name: name}, true
	}
	// If there is a { then read labels
	if p.stream[p.idx] == '{' {
		p.idx += 1

		labels := []LabelFilter{}
		label, good := p.parseLabelFilter()
		if good {
			labels = append(labels, label)
		}

		for p.parseChar(',') {
			label, good := p.parseLabelFilter()
			if !good {
				p.idx = tmpIdx
				return Metric{}, false
			}
			labels = append(labels, label)
		}

		if !p.skipWhitespace() {
			p.idx = tmpIdx
			return Metric{}, false
		}

		if p.stream[p.idx] != '}' {
			p.idx = tmpIdx
			return Metric{}, false
		}
		p.idx += 1

		return Metric{Name: name, LabelFilters: labels}, true
	}
	return Metric{Name: name}, true
}

func (p *Parser) ParseAggregation() (Aggregation, bool) {
	// skip whitespace
	if !p.skipWhitespace() {
		return Aggregation{}, false
	}

	tmpIdx := p.idx
	// read ID
	op, good := p.parseID()
	if !good {
		p.idx = tmpIdx
		return Aggregation{}, false
	}

	// Check for By clause
	by, _ := p.parseBy()

	// read (
	if !p.parseChar('(') {
		p.idx = tmpIdx
		return Aggregation{}, false
	}

	// read metric
	metric, good := p.parseMetric()
	if !good {
		p.idx = tmpIdx
		return Aggregation{}, false
	}
	// read )
	if !p.parseChar(')') {
		p.idx = tmpIdx
		return Aggregation{}, false
	}

	return Aggregation{Op: op, Metric: metric, By: by}, true
}

func (p *Parser) parseBy() (By, bool) {
	// skip whitespace
	if !p.skipWhitespace() {
		return By{}, false
	}

	by_clause, good := p.parseID()
	if strings.ToLower(by_clause) != "by" || !good {
		return By{}, false
	}

	tmpIdx := p.idx
	// Read (
	if !p.parseChar('(') {
		p.idx = tmpIdx
		return By{}, false
	}

	labels := []string{}
	// Read list of label IDs
	label, good := p.parseID()
	if !good {
		p.idx = tmpIdx
		return By{}, false
	}
	labels = append(labels, label)

	for p.parseChar(',') {
		label, good := p.parseID()
		if !good {
			p.idx = tmpIdx
			return By{}, false
		}
		labels = append(labels, label)
	}

	// Read )
	if !p.parseChar(')') {
		p.idx = tmpIdx
		return By{}, false
	}

	return By{Labels: labels}, true
}

func (p *Parser) skipWhitespace() bool {
	// Skip leading white space
	for p.idx < len(p.stream) && unicode.IsSpace(p.stream[p.idx]) {
		p.idx += 1
	}

	return p.idx != len(p.stream)
}

func (p *Parser) parseChar(c rune) bool {
	// Skip leading white space
	for p.idx < len(p.stream) && unicode.IsSpace(p.stream[p.idx]) {
		p.idx += 1
	}

	if p.idx == len(p.stream) {
		return false
	}

	if p.stream[p.idx] == c {
		p.idx += 1
		return true
	}

	return false
}

/**
LOGQL Parsing logic
**/

func (p *Parser) isOperatorChar(c rune) bool {
	return c == '=' || c == '!' || c == '~' || c == '|'
}

func (p *Parser) parseLogQlOp() (string, bool) {
	// Skip leading white space
	for p.idx < len(p.stream) && unicode.IsSpace(p.stream[p.idx]) {
		p.idx += 1
	}

	if p.idx == len(p.stream) {
		return "", false
	}

	start := p.idx
	for p.idx < len(p.stream) && p.isOperatorChar(p.stream[p.idx]) {
		p.idx++
	}

	end := p.idx
	// Convert slice into string
	id := string(p.stream[start:end])

	// Update the cursor and return the string
	return id, true
}

func (p *Parser) parse() (QueryRepresentation, bool) {
	if queryRepresentation, good := p.parseLogQlQuery(); good {
		return queryRepresentation, true
	} else if queryRepresentation, good := p.ParseAggregation(); good {
		return queryRepresentation, true
	} else if queryRepresentation, good := p.parseMetric(); good {
		return queryRepresentation, true
	}

	return nil, false
}

func (p *Parser) parseLogQlQuery() (LogQlQuery, bool) {
	if !p.parseChar('{') {
		return LogQlQuery{}, false
	}

	// Parse label filters
	var labels []LabelFilter
	for !p.parseChar('}') {
		labelName, _ := p.parseID()
		operator, _ := p.parseLogQlOp()
		matchString, _ := p.parseString()

		labels = append(labels, LabelFilter{Label: labelName, Value: matchString, Op: operator})
		p.parseChar(',')
	}

	p.parseChar('}')

	// Parse log filters
	var logFilters []LabelFilter
	for p.hasChar() {
		op, result := p.parseLogQlOp()
		if result == false {
			break
		}

		matchString, _ := p.parseString()
		logFilters = append(logFilters, LabelFilter{Label: "logLine", Value: matchString, Op: op})
	}

	return LogQlQuery{labelFilters: labels, logFilters: logFilters}, true
}

func (p *Parser) hasChar() bool {
	return p.idx < len(p.stream)
}
