package pinotlib

import (
	"fmt"
	"strings"
	"time"
)

type TimeUnit string

const (
	TimeUnitDays         TimeUnit = "DAYS"
	TimeUnitHours        TimeUnit = "HOURS"
	TimeUnitMinutes      TimeUnit = "MINUTES"
	TimeUnitSeconds      TimeUnit = "SECONDS"
	TimeUnitMilliseconds TimeUnit = "MILLISECONDS"
	TimeUnitMicroseconds TimeUnit = "MICROSECONDS"
	TimeUnitNanoseconds  TimeUnit = "NANOSECONDS"
)

func ParseTimeUnit(s string) (TimeUnit, error) {
	unit := TimeUnit(strings.ToUpper(s))
	// TODO: Unsure if validate should be in this method or stay separate.
	return unit, unit.Validate()
}

func (unit TimeUnit) Validate() error {
	switch unit {
	case TimeUnitDays, TimeUnitHours, TimeUnitMinutes, TimeUnitSeconds,
		TimeUnitMilliseconds, TimeUnitMicroseconds, TimeUnitNanoseconds:
		return nil
	default:
		return fmt.Errorf("invalid time unit `%s`", unit)
	}
}

func (unit TimeUnit) String() string {
	return string(unit)
}

func (unit TimeUnit) Duration() time.Duration {
	switch unit {
	case TimeUnitNanoseconds:
		return time.Nanosecond
	case TimeUnitMicroseconds:
		return time.Microsecond
	case TimeUnitMilliseconds:
		return time.Millisecond
	case TimeUnitSeconds:
		return time.Second
	case TimeUnitMinutes:
		return time.Minute
	case TimeUnitHours:
		return time.Hour
	case TimeUnitDays:
		return time.Hour * 24
	default:
		return 0
	}
}
