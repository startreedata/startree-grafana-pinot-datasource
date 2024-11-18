package pinotlib

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

var dateTimeConvertRegex = regexp.MustCompile(`(?i)^DATETIMECONVERT\s*\(\s*(\S+)\s*,\s*'(\S+)'\s*,\s*'(\S+)'\s*,\s*'(\S+)'\s*\)$`)
var epochBucketRegex = regexp.MustCompile(`(?i)^FromEpoch(\w+)Bucket\s*\(\s*ToEpoch(\w+)Bucket\s*\(\s*(\S+)\s*,\s*(\d+)\s*\)\s*,\s*(\d+)\s*\)$`)

type TimeGroupExpression struct {
	timeColumn   string
	inputFormat  PinotDateTimeFormat
	outputFormat PinotDateTimeFormat
	granularity  PinotGranularity
}

func ParseTimeGroupExpression(expr string) (TimeGroupExpression, error) {
	expr = strings.TrimSpace(expr)

	matchAndArgs := dateTimeConvertRegex.FindStringSubmatch(expr)
	if len(matchAndArgs) == 5 {
		columnName := UnquoteObjectName(matchAndArgs[1])
		inputFormat, err := ParsePinotDateTimeFormat(matchAndArgs[2])
		if err != nil {
			return TimeGroupExpression{}, err
		}
		outputFormat, err := ParsePinotDateTimeFormat(matchAndArgs[3])
		if err != nil {
			return TimeGroupExpression{}, err
		}
		granularity, err := ParsePinotGranularity(matchAndArgs[4])
		if err != nil {
			return TimeGroupExpression{}, err
		}
		return TimeGroupExpression{
			timeColumn:   columnName,
			inputFormat:  inputFormat,
			outputFormat: outputFormat,
			granularity:  granularity,
		}, nil
	}

	matchAndArgs = epochBucketRegex.FindStringSubmatch(expr)
	if len(matchAndArgs) == 6 {
		fromUnit, err := ParseTimeUnit(matchAndArgs[1])
		if err != nil {
			return TimeGroupExpression{}, err
		}

		toUnit, err := ParseTimeUnit(matchAndArgs[2])
		if err != nil {
			return TimeGroupExpression{}, err
		}

		columnName := UnquoteObjectName(matchAndArgs[3])
		toSize := matchAndArgs[4]
		fromSize := matchAndArgs[5]

		if fromUnit != toUnit || toSize != fromSize {
			return TimeGroupExpression{}, errors.New("invalid time group expression")
		}

		size, err := strconv.ParseUint(toSize, 10, 64)
		if err != nil {
			return TimeGroupExpression{}, err
		}
		granularity, err := NewPinotGranularity(fromUnit, uint(size))
		if err != nil {
			return TimeGroupExpression{}, err
		}

		return TimeGroupExpression{
			timeColumn: columnName,
			inputFormat: PinotDateTimeFormat{
				Size:   1,
				Unit:   TimeUnitMilliseconds,
				Format: TimeFormatEpoch,
			},
			outputFormat: PinotDateTimeFormat{
				Size:   1,
				Unit:   TimeUnitMilliseconds,
				Format: TimeFormatEpoch,
			},
			granularity: granularity,
		}, nil
	}
	return TimeGroupExpression{}, errors.New("invalid time group expression")
}

func (x TimeGroupExpression) Equals(expr TimeGroupExpression) bool {
	return x.timeColumn == expr.timeColumn &&
		x.inputFormat.Equals(expr.inputFormat) &&
		x.outputFormat.Equals(expr.outputFormat) &&
		x.granularity.Equals(expr.granularity)
}
