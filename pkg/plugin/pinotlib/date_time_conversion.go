package pinotlib

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

var dateTimeConvertRegex = regexp.MustCompile(`(?i)^DATETIMECONVERT\s*\(\s*(\S+)\s*,\s*'(\S+)'\s*,\s*'(\S+)'\s*,\s*'(\S+)'\s*\)$`)
var epochBucketRegex = regexp.MustCompile(`(?i)^FromEpoch(\w+)Bucket\s*\(\s*ToEpoch(\w+)Bucket\s*\(\s*(\S+)\s*,\s*(\d+)\s*\)\s*,\s*(\d+)\s*\)$`)

type DateTimeConversion struct {
	TimeColumn   string
	InputFormat  DateTimeFormat
	OutputFormat DateTimeFormat
	Granularity  Granularity
}

func ParseDateTimeConversionExpr(expr string) (DateTimeConversion, error) {
	expr = strings.TrimSpace(expr)

	if match := dateTimeConvertRegex.FindStringSubmatch(expr); len(match) == 5 {
		return DateTimeConversionOf(UnquoteObjectName(match[1]), match[2], match[3], match[4])
	}

	if match := epochBucketRegex.FindStringSubmatch(expr); len(match) == 6 {
		fromUnit, err := ParseTimeUnit(match[1])
		if err != nil {
			return DateTimeConversion{}, err
		}

		toUnit, err := ParseTimeUnit(match[2])
		if err != nil {
			return DateTimeConversion{}, err
		}

		columnName := UnquoteObjectName(match[3])
		toSize := match[4]
		fromSize := match[5]

		if fromUnit != toUnit || toSize != fromSize {
			return DateTimeConversion{}, errors.New("invalid time group expression")
		}

		size, err := strconv.ParseUint(fromSize, 10, 64)
		if err != nil {
			return DateTimeConversion{}, err
		}

		return DateTimeConversion{
			TimeColumn:   columnName,
			InputFormat:  DateTimeFormatMillisecondsEpoch(),
			OutputFormat: DateTimeFormatMillisecondsEpoch(),
			Granularity:  Granularity{Unit: fromUnit, Size: uint(size)},
		}, nil
	}
	return DateTimeConversion{}, errors.New("invalid time group expression")
}

func DateTimeConversionOf(timeColumn, inputFormat, outputFormat, granularity string) (DateTimeConversion, error) {
	parsedInputFormat, err := ParseDateTimeFormat(inputFormat)
	if err != nil {
		return DateTimeConversion{}, err
	}
	parsedOutputFormat, err := ParseDateTimeFormat(outputFormat)
	if err != nil {
		return DateTimeConversion{}, err
	}
	parsedGranularity, err := ParseGranularityExpr(granularity)
	if err != nil {
		return DateTimeConversion{}, err
	}
	return DateTimeConversion{
		TimeColumn:   timeColumn,
		InputFormat:  parsedInputFormat,
		OutputFormat: parsedOutputFormat,
		Granularity:  parsedGranularity,
	}, nil
}

func (x DateTimeConversion) Equals(expr DateTimeConversion) bool {
	return x.TimeColumn == expr.TimeColumn &&
		x.InputFormat.Equals(expr.InputFormat) &&
		x.OutputFormat.Equals(expr.OutputFormat) &&
		x.Granularity.Equals(expr.Granularity)
}

type DerivedTimeColumn struct {
	ColumnName string
	Source     DateTimeConversion
}

func DerivedTimeColumnsFrom(config TableConfig) []DerivedTimeColumn {
	var derivedTimeColumns []DerivedTimeColumn
	for _, transform := range config.RealTime.IngestionConfig.TransformConfigs {
		timeGroup, err := ParseDateTimeConversionExpr(transform.TransformFunction)
		if err != nil {
			continue
		}
		derivedTimeColumns = append(derivedTimeColumns, DerivedTimeColumn{
			ColumnName: transform.ColumnName,
			Source:     timeGroup,
		})
	}
	return derivedTimeColumns
}
