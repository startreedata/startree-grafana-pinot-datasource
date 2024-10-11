package dataquery

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestExtractTimeSeriesMetrics(t *testing.T) {

}

func TestFormatSeriesName(t *testing.T) {
	type Args struct {
		defaultName string
		legend      string
		labels      map[string]string
	}

	testCases := []struct {
		name string
		args Args
		want string
	}{
		{
			name: "legend={{city }}-{{ state}}-{{ dim.Region&&*##@name }}",
			args: Args{
				legend: "{{city }}-{{ state}}-{{ dim.Region&&*##@name }}",
				labels: map[string]string{
					"city":                 "Albany",
					"state":                "New York",
					"dim.Region&&*##@name": "Region_C",
				},
			},
			want: "Albany-New York-Region_C",
		},
		{
			name: "missing labels",
			args: Args{
				legend: "{{city }}-{{ state}}-{{ dim.Region&&*##@name }}",
				labels: map[string]string{
					"dim.Region&&*##@name": "Region_C",
				},
			},
			want: "{{city }}-{{ state}}-Region_C",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatSeriesName(tt.args.legend, tt.args.labels)
			assert.Equal(t, tt.want, got)
		})
	}

}
