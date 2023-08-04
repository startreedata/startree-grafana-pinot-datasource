package plugin

import "testing"

func TestParsePromQL(t *testing.T) {

}

func TestParseString(t *testing.T) {
	for _, test := range []string{"\"test\"", " \"test\"", "\"test\" ", " \"test\" ", "\"test\"()"} {
		parser := CreateParser(test)
		str, good := parser.parseString()

		if str != "test" || !good {
			t.Fatalf("Expected %s but got %s", test, str)
		}
	}

	for _, test := range []string{"test", "", "\"", " \"test", "test\""} {
		parser := CreateParser(test)
		str, good := parser.parseString()

		if good {
			t.Fatalf("Expected %s but got %s", test, str)
		}
	}
}

func TestParseID(t *testing.T) {
	for _, test := range []string{"test", " test", "test ", " test ", "test()"} {
		parser := CreateParser(test)
		id, good := parser.parseID()

		if id != "test" || !good {
			t.Fatalf("Expected %s but got %s", test, id)
		}
	}

	for _, test := range []string{"", "  ", "()", " (test ", "()test"} {
		parser := CreateParser(test)
		id, good := parser.parseID()

		if good {
			t.Fatalf("'%s': Expected no match but got %s", test, id)
		}
	}
}

func TestParseLabelFilter(t *testing.T) {
	// <id>:<string>
	for _, test := range []string{
		"label:\"value\"",
		" label:\"value\"",
		"label : \"value\"",
		"label: \"value\" ",
		"label: \"value\"() ",
	} {
		parser := CreateParser(test)
		filter, good := parser.parseLabelFilter()

		if filter.Label != "label" || filter.Value != "value" || !good {
			t.Fatalf("%s: Invalid label", test)
		}
	}

	for _, test := range []string{"", "  ", "()", " (test ", "()test", "label: ", ":\"value\"", "label:value"} {
		parser := CreateParser(test)
		id, good := parser.parseLabelFilter()

		if good {
			t.Fatalf("'%s': Expected no match but got %s", test, id)
		}
	}
}

func TestParsePlainMetric(t *testing.T) {
	// <id>
	for _, test := range []string{
		"metric",
		" metric",
		"metric ",
		"metric {} ",
		"  metric() ",
	} {
		parser := CreateParser(test)
		metric, good := parser.parseMetric()

		if metric.Name != "metric" || !good {
			t.Fatalf("%s: Invalid metric", test)
		}
	}

	for _, test := range []string{
		"",
		" (metric)",
		"\"metric\"",
		"1143",
	} {
		parser := CreateParser(test)
		_, good := parser.parseMetric()

		if good {
			t.Fatalf("%s: Expected this to fail", test)
		}
	}
}

func TestParseMetricWithLabels(t *testing.T) {
	// <id>{<label_filter>}
	for _, test := range []string{
		"metric{label:\"value\"}",
		"metric {label:\"value\"}",
		"metric { label:\"value\"}",
		"metric { label:\"value\" }",
		"metric { label:\"value\"} ",
	} {
		parser := CreateParser(test)
		metric, good := parser.parseMetric()

		if !(metric.Name == "metric" && len(metric.LabelFilters) == 1) || !good {
			t.Fatalf("%s: Invalid metric got %s, %s", test, metric.Name, metric.LabelFilters)
		}
	}

	for _, test := range []string{
		"metric{\"value\"}",
		"metric {:}",
		"metric { label:}",
		"metric { label:\" }",
		"metric { label:\"value\" ",
	} {
		parser := CreateParser(test)
		_, good := parser.parseMetric()

		if good {
			t.Fatalf("%s: Expected Failure", test)
		}
	}
}

func TestParseBy(t *testing.T) {
	// by(<id>)
}

func TestParseAggregation(t *testing.T) {
	// operator (<metric>)
	for _, test := range []string{
		"avg(metric)",
		" avg(metric)",
		"avg( metric )",
		"avg( metric{} )",
	} {
		parser := CreateParser(test)
		agg, good := parser.ParseAggregation()

		if !(agg.Op == "avg" && agg.Metric.Name == "metric" && len(agg.Metric.LabelFilters) == 0) || !good {
			t.Fatalf("%s: Invalid metric", test)
		}
	}

	for _, test := range []string{
		"avg(metric{label: \"test\"})",
		" avg(metric{label: \"test\"})",
		"avg( metric {label: \"test\"})",
	} {
		parser := CreateParser(test)
		agg, good := parser.ParseAggregation()

		if !(agg.Op == "avg" && agg.Metric.Name == "metric" && len(agg.Metric.LabelFilters) == 1) || !good {
			t.Fatalf("%s: Invalid metric", test)
		}
	}
}

func TestParseAggregationWithBy(t *testing.T) {
	// avg by(<id>) (<metric>)
	for _, test := range []string{
		"avg by(label) (metric)",
		" avg by(label) (metric)",
		"avg by(label)( metric )",
	} {
		parser := CreateParser(test)
		agg, good := parser.ParseAggregation()

		if !(agg.Op == "avg" && agg.Metric.Name == "metric" && len(agg.By.Labels) == 1 && len(agg.Metric.LabelFilters) == 0) || !good {
			t.Fatalf("%s: Invalid metric", test)
		}
	}

	// avg by(<id>[,<id>]) (<metric>)
	for _, test := range []string{
		"avg by(label, label2) (metric)",
		" avg by(label, label2) (metric)",
		"avg by(label, label2)( metric )",
	} {
		parser := CreateParser(test)
		agg, good := parser.ParseAggregation()

		if !(agg.Op == "avg" && agg.Metric.Name == "metric" && len(agg.By.Labels) == 2 && len(agg.Metric.LabelFilters) == 0) || !good {
			t.Fatalf("%s: Invalid metric", test)
		}
	}
}
