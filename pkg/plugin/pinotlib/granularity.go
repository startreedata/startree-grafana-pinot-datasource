package pinotlib

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Granularity struct {
	Unit TimeUnit
	Size uint
}

func NewPinotGranularity(unit TimeUnit, size uint) (Granularity, error) {
	if size == 0 {
		return Granularity{}, fmt.Errorf("size must be > 0")
	}

	return Granularity{unit, size}, nil
}

func GranularityOf(duration time.Duration) Granularity {
	switch {
	case duration.Hours() >= 1:
		return Granularity{Unit: TimeUnitHours, Size: uint(duration.Hours())}
	case duration.Minutes() >= 1:
		return Granularity{Unit: TimeUnitMinutes, Size: uint(duration.Minutes())}
	case duration.Seconds() >= 1:
		return Granularity{Unit: TimeUnitSeconds, Size: uint(duration.Seconds())}
	case duration.Milliseconds() >= 1:
		return Granularity{Unit: TimeUnitMilliseconds, Size: uint(duration.Milliseconds())}
	case duration.Microseconds() >= 1:
		return Granularity{Unit: TimeUnitMicroseconds, Size: uint(duration.Microseconds())}
	default:
		return Granularity{Unit: TimeUnitNanoseconds, Size: uint(duration.Nanoseconds())}
	}
}

func ParseGranularityExpr(granularity string) (Granularity, error) {
	var size uint64
	var unit TimeUnit
	var err error

	fields := strings.SplitN(granularity, ":", 2)
	if len(fields) == 1 {
		size = 1

		unit, err = ParseTimeUnit(fields[0])
		if err != nil {
			return Granularity{}, fmt.Errorf("failed to parse granularity `%s`: %w", granularity, err)
		}
	} else {
		size, err = strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return Granularity{}, fmt.Errorf("failed to parse granularity `%s`: %w", granularity, err)
		}

		unit, err = ParseTimeUnit(fields[1])
		if err != nil {
			return Granularity{}, fmt.Errorf("failed to parse granularity `%s`: %w", granularity, err)
		}
	}

	return Granularity{Unit: unit, Size: uint(size)}, nil
}

func (x Granularity) String() string {
	return fmt.Sprintf("%d:%s", x.Size, x.Unit)
}

func (x Granularity) Duration() time.Duration {
	return time.Duration(x.Size) * x.Unit.Duration()
}

func (x Granularity) Equals(g Granularity) bool {
	return x.Duration() == g.Duration()
}
