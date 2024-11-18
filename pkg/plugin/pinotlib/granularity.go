package pinotlib

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type PinotGranularity struct {
	Unit TimeUnit
	Size uint
}

func NewPinotGranularity(unit TimeUnit, size uint) (PinotGranularity, error) {
	if size == 0 {
		return PinotGranularity{}, fmt.Errorf("size must be > 0")
	}

	return PinotGranularity{unit, size}, nil
}

func ParsePinotGranularity(granularity string) (PinotGranularity, error) {
	var size uint64
	var unit TimeUnit
	var err error

	fields := strings.SplitN(granularity, ":", 2)
	if len(fields) == 1 {
		size = 1

		unit, err = ParseTimeUnit(fields[0])
		if err != nil {
			return PinotGranularity{}, fmt.Errorf("failed to parse granularity `%s`: %w", granularity, err)
		}
	} else {
		size, err = strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return PinotGranularity{}, fmt.Errorf("failed to parse granularity `%s`: %w", granularity, err)
		}

		unit, err = ParseTimeUnit(fields[1])
		if err != nil {
			return PinotGranularity{}, fmt.Errorf("failed to parse granularity `%s`: %w", granularity, err)
		}
	}

	pinotGranularity, err := NewPinotGranularity(unit, uint(size))
	if err != nil {
		return PinotGranularity{}, fmt.Errorf("failed to parse granularity `%s`: %w", granularity, err)
	}
	return pinotGranularity, nil
}

func (x PinotGranularity) String() string {
	return fmt.Sprintf("%d:%s", x.Size, x.Unit)
}

func (x PinotGranularity) Duration() time.Duration {
	return time.Duration(x.Size) * x.Unit.Duration()
}

func (x PinotGranularity) Equals(g PinotGranularity) bool {
	return x.Duration() == g.Duration()
}
