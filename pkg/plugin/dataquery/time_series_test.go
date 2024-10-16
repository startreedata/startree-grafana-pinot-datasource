package dataquery

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

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

	var formatter LegendFormatter
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			got := formatter.FormatSeriesName(tt.args.legend, tt.args.labels)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetSeriesKey(t *testing.T) {
	// Make a matrix of unique labels
	labelsCount := 5
	dimCount := 5
	var labelsMatrix [][]MetricLabel

	for i := 0; i < labelsCount; i++ {
		var labels []MetricLabel
		for j := 0; j < dimCount; j++ {
			labels = append(labels, MetricLabel{
				name: fmt.Sprintf("dim_%d", j),
				// It's valid for different labels to have the same value.
				value: fmt.Sprintf("value_%d", i),
			})
		}
		labelsMatrix = append(labelsMatrix, labels)
	}

	t.Run("no labels", func(t *testing.T) {
		var seriesMapper SeriesMapper
		assert.Equal(t, 0, seriesMapper.GetKey(nil))
	})

	t.Run("different sizes", func(t *testing.T) {
		var seriesMapper SeriesMapper

		assert.Panics(t, func() {
			seriesMapper.GetKey([]MetricLabel{{name: "a"}})
			seriesMapper.GetKey([]MetricLabel{{name: "a"}, {name: "b"}})
		})
	})

	t.Run("different names", func(t *testing.T) {
		var seriesMapper SeriesMapper

		assert.Panics(t, func() {
			seriesMapper.GetKey([]MetricLabel{{name: "a"}, {name: "b"}})
			seriesMapper.GetKey([]MetricLabel{{name: "a"}, {name: "c"}})
		})
	})

	t.Run("different order", func(t *testing.T) {
		var seriesMapper SeriesMapper

		assert.Panics(t, func() {
			seriesMapper.GetKey([]MetricLabel{{name: "a"}, {name: "b"}})
			seriesMapper.GetKey([]MetricLabel{{name: "b"}, {name: "a"}})
		})
	})

	t.Run("first insert", func(t *testing.T) {
		var seriesMapper SeriesMapper

		// Since all the labels are unique and this is the first encounter, the key should be the # of invocations.
		for i, labels := range labelsMatrix {
			assert.Equal(t, i, seriesMapper.GetKey(labels), labels)
		}
	})

	t.Run("retrieval", func(t *testing.T) {
		var seriesMapper SeriesMapper

		// Repeated labels should always return the same value, so we repeat the loop a few times.
		for x := 0; x < 3; x++ {
			for i, labels := range labelsMatrix {
				assert.Equal(t, i, seriesMapper.GetKey(labels), labels)
			}
		}
	})
}
