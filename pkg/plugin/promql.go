package plugin

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/grafana/loki/pkg/logql"
	"github.com/prometheus/prometheus/pkg/labels"
)

func AggToSql(table string, interval int64, agg Aggregation, from, to int64) string {
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

func MetricToSql(table string, interval int64, metric Metric, from, to int64) string {
	return fmt.Sprintf(
		`SELECT min("time") as "time", avg(value) as value, floor("time" / %d) as bucket 
			 FROM %s 
			 %s
			 GROUP BY bucket 
			 ORDER BY bucket ASC
			 LIMIT 1000`,
		interval, table, filterToWhereClause(metric.Name, interval, from, to, metric.LabelFilters))
}

func LogQlToSql(table string, interval int64, logQl LogQl, from, to int64) string {
	return fmt.Sprintf(
		`SELECT floor("time" / %d) as bucket, logLine, 
			 FROM %s 
			 %s
			 GROUP BY bucket 
			 ORDER BY bucket ASC
			 LIMIT 1000`,
		interval, table, logQl.predicate)
}

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
}

type Metric struct {
	Name         string
	LabelFilters []LabelFilter
}

type LogQl struct {
	predicate string
}

type Aggregation struct {
	Op     string
	Metric Metric
	By     By
}

type By struct {
	Labels []string
}

type Parser struct {
	idx    int
	query  string
	stream []rune
}

func CreateParser(text string) Parser {
	return Parser{
		idx:    0,
		query:  text,
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

func (p *Parser) parseLogQl() (LogQl, bool) {
	parsedExpr, err := logql.ParseLogSelector(p.query)
	if err != nil {
		return LogQl{}, false
	}

	return LogQl{p.convertLogQLExprToPinotPredicate(parsedExpr)}, true
}

// logQL ast -> pinot predicates
func (p *Parser) convertLogQLExprToPinotPredicate(expr logql.LogSelectorExpr) string {
	matchers := expr.Matchers()
	// filters, _ := expr.Filter()
	var sb strings.Builder

	for _, matcher := range matchers {
		var predicate string
		switch matcher.Type {
		case labels.MatchEqual:
			predicate = matcher.Name + " = '" + matcher.Value + "'"
			break
		case labels.MatchNotEqual:
			predicate = matcher.Name + " != '" + matcher.Value + "'"
			break
		case labels.MatchNotRegexp:
			predicate = "REGEXP_NOT_LIKE(" + matcher.Name + ", '" + matcher.Value + "')"
			break
		case labels.MatchRegexp:
			predicate = "REGEXP_LIKE(" + matcher.Name + ", '" + matcher.Value + "')"
			break
		}
		if sb.Len() == 0 {
			// First one
			sb.WriteString(predicate)
		} else {
			sb.WriteString(" AND " + predicate)
		}
	}

	filterPredicates := extractFilterPredicates(expr.String())

	if len(filterPredicates) > 0 {
		if sb.Len() == 0 {
			// First one
			sb.WriteString(filterPredicates)
		} else {
			sb.WriteString(" AND " + filterPredicates)
		}
	}

	return sb.String()
}

func isOperatorCharacter(expr string, position int) bool {
	return []rune(expr)[position] == '|' || []rune(expr)[position] == '!'
}

func extractFilterPredicates(parsedExpr string) string {
	// Find last occurance of '}' character
	var predicaetBuilder strings.Builder

	labelMatcherEnd := strings.LastIndex(parsedExpr, "}")
	labelMatcherEnd++

	var operator string
	var predicate string
	for labelMatcherEnd < len(parsedExpr) {
		operator = parsedExpr[labelMatcherEnd : labelMatcherEnd+2]
		var literalStart = labelMatcherEnd + 2
		var literalEnd = labelMatcherEnd + 2
		for literalEnd < len(parsedExpr) && !isOperatorCharacter(parsedExpr, literalEnd) {
			literalEnd++
		}

		var literal = parsedExpr[literalStart:literalEnd]
		labelMatcherEnd = literalEnd
		switch operator {
		case "|=": // Contains
			predicate = "TEXT_CONTAINS(logLine, '" + literal + "')"
		case "!=": // Not contains
			predicate = "!TEXT_CONTAINS(logLine, '" + literal + "')"
		case "|~": // REGEXP_LIKE
			predicate = "REGEXP_LIKE(logLine, '" + literal + "')"
		case "!~": // REGEXP_NOT_LIKE
			predicate = "REGEXP_NOT_LIKE(logLine, '" + literal + "')"
		}

		if predicaetBuilder.Len() == 0 {
			// First one
			predicaetBuilder.WriteString(predicate)
		} else {
			predicaetBuilder.WriteString(" AND " + predicate)
		}
	}

	return predicaetBuilder.String()
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
