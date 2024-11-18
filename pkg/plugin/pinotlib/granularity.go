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

func NewPinotGranularity(unit string, size uint) (PinotGranularity, error) {
	if size == 0 {
		return PinotGranularity{}, fmt.Errorf("size must be > 0")
	}

	timeUnit, err := ParseTimeUnit(unit)
	if err != nil {
		return PinotGranularity{}, err
	}
	return PinotGranularity{timeUnit, size}, nil
}

func ParsePinotGranularity(granularity string) (PinotGranularity, error) {
	var size uint64
	var unit string
	fields := strings.SplitN(granularity, ":", 2)
	if len(fields) == 1 {
		unit = fields[0]
		size = 1
	} else {
		var err error
		if size, err = strconv.ParseUint(fields[0], 10, 64); err != nil {
			return PinotGranularity{}, fmt.Errorf("failed to parse granularity `%s`: %w", granularity, err)
		}
		unit = fields[1]
	}
	pinotGranularity, err := NewPinotGranularity(unit, uint(size))
	if err != nil {
		return PinotGranularity{}, fmt.Errorf("failed to parse granularity `%s`: %w", granularity, err)
	}
	return pinotGranularity, nil
}

func (x *PinotGranularity) String() string {
	return fmt.Sprintf("%d:%s", x.Size, x.Unit)
}

func (x *PinotGranularity) Duration() time.Duration {
	return time.Duration(x.Size) * x.Unit.Duration()
}
