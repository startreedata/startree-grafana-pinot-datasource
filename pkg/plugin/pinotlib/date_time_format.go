package pinotlib

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Ref: https://docs.pinot.apache.org/configuration-reference/schema#new-datetime-formats

type DateTimeFormat struct {
	Size   uint
	Unit   TimeUnit
	Format TimeFormat
}

func DateTimeFormatMillisecondsEpoch() DateTimeFormat {
	return DateTimeFormat{
		Size:   1,
		Unit:   TimeUnitMilliseconds,
		Format: TimeFormatEpoch,
	}
}

func ParseDateTimeFormat(format string) (DateTimeFormat, error) {
	// Common time formats and shorthands.
	switch format {
	case "EPOCH_NANOS", "1:NANOSECONDS:EPOCH", "EPOCH|NANOSECONDS", "EPOCH|NANOSECONDS|1":
		return DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitNanoseconds}, nil
	case "EPOCH_MICROS", "1:MICROSECONDS:EPOCH", "EPOCH|MICROSECONDS", "EPOCH|MICROSECONDS|1":
		return DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMicroseconds}, nil
	case "EPOCH", "EPOCH_MILLIS", "TIMESTAMP", "1:MILLISECONDS:EPOCH", "EPOCH|MILLISECONDS", "EPOCH|MILLISECONDS|1":
		return DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMilliseconds}, nil
	case "EPOCH_SECONDS", "1:SECONDS:EPOCH", "EPOCH|SECONDS", "EPOCH|SECONDS|1":
		return DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitSeconds}, nil
	case "EPOCH_MINUTES", "1:MINUTES:EPOCH", "EPOCH|MINUTES", "EPOCH|MINUTES|1":
		return DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMinutes}, nil
	case "EPOCH_HOURS", "1:HOURS:EPOCH", "EPOCH|HOURS", "EPOCH|HOURS|1":
		return DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitHours}, nil
	case "EPOCH_DAYS", "1:DAYS:EPOCH", "EPOCH|DAYS", "EPOCH|DAYS|1":
		return DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitDays}, nil
	}

	if isV0_12DateTimeFormat(format) {
		return parseV0_12DateTimeFormat(format)
	} else {
		return parseLegacyDateTimeFormat(format)
	}
}

func (x DateTimeFormat) Equals(dt DateTimeFormat) bool {
	return x.Format == dt.Format && x.IntervalSize() == dt.IntervalSize()
}

func (x DateTimeFormat) IntervalSize() time.Duration {
	return Granularity{Unit: x.Unit, Size: x.Size}.Duration()
}

// TODO: This should not be a method on dtf.
func (x DateTimeFormat) ParseLong(v int64) time.Time {
	switch x.Unit {
	case TimeUnitDays:
		return time.Unix(86400*v*int64(x.Size), 0).UTC()
	case TimeUnitHours:
		return time.Unix(3600*v*int64(x.Size), 0).UTC()
	case TimeUnitMinutes:
		return time.Unix(60*v*int64(x.Size), 0).UTC()
	case TimeUnitSeconds:
		return time.Unix(v*int64(x.Size), 0).UTC()
	case TimeUnitMilliseconds:
		return time.UnixMilli(v * int64(x.Size)).UTC()
	case TimeUnitMicroseconds:
		return time.UnixMicro(v * int64(x.Size)).UTC()
	default:
		return time.Unix(0, v*int64(x.Size)).UTC()
	}
}

func (x DateTimeFormat) LegacyString() string {
	return fmt.Sprintf("%d:%s:%s", x.Size, x.Unit, x.Format)
}

func (x DateTimeFormat) V0_12String() string {
	return fmt.Sprintf("%s|%s|%d", x.Format, x.Unit, x.Size)
}

func isV0_12DateTimeFormat(format string) bool {
	return strings.HasPrefix(format, TimeFormatEpoch.String()) || strings.HasPrefix(format, TimeFormatSimpleDateFormat.String())
}

func parseV0_12DateTimeFormat(format string) (DateTimeFormat, error) {
	fields := strings.SplitN(format, "|", 3)

	if _, err := parseTimeFormat(fields[0]); err != nil {
		return DateTimeFormat{}, fmt.Errorf("failed to parse date time format `%s`: %w", format, err)
	}

	if len(fields) == 1 {
		return DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMilliseconds}, nil
	}

	unit, err := ParseTimeUnit(fields[1])
	if err != nil {
		return DateTimeFormat{}, fmt.Errorf("failed to parse date time format `%s`: %w", format, err)
	}

	if len(fields) == 2 {
		return DateTimeFormat{
			Size: 1,
			Unit: unit,
		}, nil
	}

	size, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return DateTimeFormat{}, fmt.Errorf("failed to parse date time format `%s`: %w", format, err)
	}

	return DateTimeFormat{Format: TimeFormatEpoch, Size: uint(size), Unit: unit}, nil
}

func parseLegacyDateTimeFormat(format string) (DateTimeFormat, error) {
	fields := strings.SplitN(format, ":", 4)
	if len(fields) < 3 {
		return DateTimeFormat{}, fmt.Errorf("invalid date time format `%s`", format)
	}

	if _, err := parseTimeFormat(fields[2]); err != nil {
		return DateTimeFormat{}, fmt.Errorf("failed to parse date time format `%s`: %w", format, err)
	}

	size, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return DateTimeFormat{}, fmt.Errorf("failed to parse date time format `%s`: %w", format, err)
	}

	unit, err := ParseTimeUnit(fields[1])
	if err != nil {
		return DateTimeFormat{}, fmt.Errorf("failed to parse date time format `%s`: %w", format, err)
	}

	return DateTimeFormat{Format: TimeFormatEpoch, Size: uint(size), Unit: unit}, nil
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
