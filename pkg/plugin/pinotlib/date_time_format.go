package pinotlib

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type PinotDateTimeFormat struct {
	Size   uint
	Unit   TimeUnit
	Format TimeFormat
}

func (x PinotDateTimeFormat) FormatTime(ts time.Time) string {
	switch x.Unit {
	case TimeUnitNanoseconds:
		return fmt.Sprintf("%d", ts.UnixNano()/int64(x.Size))
	case TimeUnitMicroseconds:
		return fmt.Sprintf("%d", ts.UnixMicro()/int64(x.Size))
	case TimeUnitMilliseconds:
		return fmt.Sprintf("%d", ts.UnixMilli()/int64(x.Size))
	case TimeUnitSeconds:
		return fmt.Sprintf("%d", ts.Unix()/int64(x.Size))
	case TimeUnitMinutes:
		return fmt.Sprintf("%d", ts.Unix()/int64(x.Size)/60)
	case TimeUnitHours:
		return fmt.Sprintf("%d", ts.Unix()/int64(x.Size)/3600)
	case TimeUnitDays:
		return fmt.Sprintf("%d", ts.Unix()/int64(x.Size)/86400)
	default:
		return ""
	}
}

func (x PinotDateTimeFormat) LegacyString() string {
	return fmt.Sprintf("%d:%s:%s", x.Size, x.Unit, x.Format)
}

func (x PinotDateTimeFormat) V0_12String() string {
	return fmt.Sprintf("%s|%s|%d", x.Format, x.Unit, x.Size)
}

func ParsePinotDateTimeFormat(format string) (PinotDateTimeFormat, error) {
	// Common time formats and shorthands.
	switch format {
	case "EPOCH_NANOS", "1:NANOSECONDS:EPOCH", "EPOCH|NANOSECONDS", "EPOCH|NANOSECONDS|1":
		return PinotDateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitNanoseconds}, nil
	case "EPOCH_MICROS", "1:MICROSECONDS:EPOCH", "EPOCH|MICROSECONDS", "EPOCH|MICROSECONDS|1":
		return PinotDateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMicroseconds}, nil
	case "EPOCH", "EPOCH_MILLIS", "TIMESTAMP", "1:MILLISECONDS:EPOCH", "EPOCH|MILLISECONDS", "EPOCH|MILLISECONDS|1":
		return PinotDateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMilliseconds}, nil
	case "EPOCH_SECONDS", "1:SECONDS:EPOCH", "EPOCH|SECONDS", "EPOCH|SECONDS|1":
		return PinotDateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitSeconds}, nil
	case "EPOCH_MINUTES", "1:MINUTES:EPOCH", "EPOCH|MINUTES", "EPOCH|MINUTES|1":
		return PinotDateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMinutes}, nil
	case "EPOCH_HOURS", "1:HOURS:EPOCH", "EPOCH|HOURS", "EPOCH|HOURS|1":
		return PinotDateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitHours}, nil
	case "EPOCH_DAYS", "1:DAYS:EPOCH", "EPOCH|DAYS", "EPOCH|DAYS|1":
		return PinotDateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitDays}, nil
	}

	if isV0_12DateTimeFormat(format) {
		return parseV0_12DateTimeFormat(format)
	} else {
		return parseLegacyDateTimeFormat(format)
	}
}

func isV0_12DateTimeFormat(format string) bool {
	return strings.HasPrefix(format, TimeFormatEpoch.String()) || strings.HasPrefix(format, TimeFormatSimpleDateFormat.String())
}

func parseV0_12DateTimeFormat(format string) (PinotDateTimeFormat, error) {
	fields := strings.SplitN(format, "|", 3)

	if _, err := parseTimeFormat(fields[0]); err != nil {
		return PinotDateTimeFormat{}, fmt.Errorf("failed to parse date time format `%s`: %w", format, err)
	}

	if len(fields) == 1 {
		return PinotDateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMilliseconds}, nil
	}

	unit, err := ParseTimeUnit(fields[1])
	if err != nil {
		return PinotDateTimeFormat{}, fmt.Errorf("failed to parse date time format `%s`: %w", format, err)
	}

	if len(fields) == 2 {
		return PinotDateTimeFormat{
			Size: 1,
			Unit: unit,
		}, nil
	}

	size, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return PinotDateTimeFormat{}, fmt.Errorf("failed to parse date time format `%s`: %w", format, err)
	}

	return PinotDateTimeFormat{Format: TimeFormatEpoch, Size: uint(size), Unit: unit}, nil
}

func parseLegacyDateTimeFormat(format string) (PinotDateTimeFormat, error) {
	fields := strings.SplitN(format, ":", 4)
	if len(fields) < 3 {
		return PinotDateTimeFormat{}, fmt.Errorf("invalid date time format `%s`", format)
	}

	if _, err := parseTimeFormat(fields[2]); err != nil {
		return PinotDateTimeFormat{}, fmt.Errorf("failed to parse date time format `%s`: %w", format, err)
	}

	size, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return PinotDateTimeFormat{}, fmt.Errorf("failed to parse date time format `%s`: %w", format, err)
	}

	unit, err := ParseTimeUnit(fields[1])
	if err != nil {
		return PinotDateTimeFormat{}, fmt.Errorf("failed to parse date time format `%s`: %w", format, err)
	}

	return PinotDateTimeFormat{Format: TimeFormatEpoch, Size: uint(size), Unit: unit}, nil
}

type TimeFormat string

const (
	TimeFormatEpoch            TimeFormat = "EPOCH"
	TimeFormatSimpleDateFormat TimeFormat = "SIMPLE_DATE_FORMAT"
)

func (f TimeFormat) String() string {
	return string(f)
}

func parseTimeFormat(format string) (TimeFormat, error) {
	timeFormat := TimeFormat(strings.ToUpper(format))
	switch timeFormat {
	case TimeFormatEpoch:
		return TimeFormatEpoch, nil
	case TimeFormatSimpleDateFormat:
		return "", fmt.Errorf("simple date time not supported")
	default:
		return "", fmt.Errorf("invalid time format `%s`", timeFormat)
	}
}
