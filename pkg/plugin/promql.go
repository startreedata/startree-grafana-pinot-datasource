package plugin

import (
	"unicode"
)

type LabelFilter struct {
	Label string
	Value string
}

type Metric struct {
	Name string
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
	if unicode.IsDigit(p.stream[p.idx]) {
		return "", false
	}

	// read until whitespace or non-alphanumeric
	for p.idx < len(p.stream) && (unicode.IsLetter(p.stream[p.idx]) || unicode.IsDigit(p.stream[p.idx])) {
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
	end := p.idx
	// If EOF then return false
	if p.idx == len(p.stream) || p.stream[p.idx] != '"' {
		return "", false
	}

	return string(p.stream[start:end]), true
}

func (p *Parser) parseLabelFilter() (LabelFilter, bool) {
	// Skip leading white space
	for p.idx < len(p.stream) && unicode.IsSpace(p.stream[p.idx]) {
		p.idx += 1
	}

	if p.idx == len(p.stream) {
		return LabelFilter{}, false
	}

	// Read ID
	label, good := p.parseID()
	if !good {
		return LabelFilter{}, false
	}
	// Read :
	if !p.parseChar(':') {
		return LabelFilter{}, false
	}
	// Read string
	value, good := p.parseString()
	if !good {
		return LabelFilter{}, false
	}

	return LabelFilter{Label: label, Value: value}, true
}

func (p *Parser) parseMetric() (Metric, bool) {
	if !p.skipWhitespace() {
		return Metric{}, false
	}

	// Read ID
	name, good := p.parseID()
	if !good {
		return Metric{}, false
	}

	return Metric{Name: name}, true
}

func (p *Parser) skipWhitespace() bool {
	// Skip leading white space
	for p.idx < len(p.stream) && unicode.IsSpace(p.stream[p.idx]) {
		p.idx += 1
	}

	if p.idx == len(p.stream) {
		return false
	}

	return true
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
