package plugin

import (
	"fmt"
	"testing"
)

func TestParsePromQL(t *testing.T) {

}

func TestParseString(t *testing.T) {
	for _, test := range []string{"\"test\"", " \"test\"", "\"test\" ", " \"test\" ", "\"test\"()"} {
		parser := CreateParser(test, "PromQL")
		str, good := parser.parseString()

		if str != "test" || !good {
			t.Fatalf("Expected %s but got %s", test, str)
		}
	}

	for _, test := range []string{"test", "", "\"", " \"test", "test\""} {
		parser := CreateParser(test, "PromQL")
		str, good := parser.parseString()

		if good {
			t.Fatalf("Expected %s but got %s", test, str)
		}
	}
}

func TestLogQlQuery(t *testing.T) {
	test := "   {   labelA = \"foo\"  ,   labelB!=\"bar\",  labelC=~\"^.baz\", labelD!~\"^..*z\" }  |= \"error\" |~ \"tsdb-ops.*io:2003\" !~ \"**d\" "

	parser := CreateParser(test, "LogQL")
	q, _ := parser.parse()
	query := q.(LogQlQuery)

	if len(query.labelFilters) != 4 {
		t.Fatalf("Expected %d but got %d", 4, len(query.labelFilters))
	}

	var labelFilters []LabelFilter
	labelFilters = append(labelFilters, LabelFilter{Label: "labelA", Value: "foo", Op: "="})
	labelFilters = append(labelFilters, LabelFilter{Label: "labelB", Value: "bar", Op: "!="})
	labelFilters = append(labelFilters, LabelFilter{Label: "labelC", Value: "^.baz", Op: "=~"})
	labelFilters = append(labelFilters, LabelFilter{Label: "labelD", Value: "^..*z", Op: "!~"})
	pinotQuery := query.toSqlQuery("myTable", 1, 1, 1)
	fmt.Println(pinotQuery)

	for i := 0; i < len(query.labelFilters); i++ {
		if query.labelFilters[i] != labelFilters[i] {
			t.Fatalf("Expected %s but got %s", labelFilters[i], query.labelFilters[i])
		}
	}

	var logFilters []LabelFilter
	logFilters = append(logFilters, LabelFilter{Label: "value", Value: "error", Op: "|="})
	logFilters = append(logFilters, LabelFilter{Label: "value", Value: "tsdb-ops.*io:2003", Op: "|~"})
	logFilters = append(logFilters, LabelFilter{Label: "value", Value: "**d", Op: "!~"})

	for i := 0; i < len(query.logFilters); i++ {
		if query.logFilters[i] != logFilters[i] {
			t.Fatalf("Expected %s but got %s", logFilters[i], query.logFilters[i])
		}
	}

}

func TestParseID(t *testing.T) {
	for _, test := range []string{"test", " test", "test ", " test ", "test()"} {
		parser := CreateParser(test, "PromQL")
		id, good := parser.parseID()

		if id != "test" || !good {
			t.Fatalf("Expected %s but got %s", test, id)
		}
	}

	for _, test := range []string{"", "  ", "()", " (test ", "()test"} {
		parser := CreateParser(test, "PromQL")
		id, good := parser.parseID()

		if good {
			t.Fatalf("'%s': Expected no match but got %s", test, id)
		}
	}
}

func TestParseLabelFilter(t *testing.T) {
	// <id>:<string>
	for _, test := range []string{
		"label=\"value\"",
		" label=\"value\"",
		"label = \"value\"",
		"label= \"value\" ",
		"label= \"value\"() ",
	} {
		parser := CreateParser(test, "PromQL")
		filter, good := parser.parseLabelFilter()

		if filter.Label != "label" || filter.Value != "value" || !good {
			t.Fatalf("%s: Invalid label", test)
		}
	}

	for _, test := range []string{"", "  ", "()", " (test ", "()test", "label: ", ":\"value\"", "label:value"} {
		parser := CreateParser(test, "PromQL")
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
		parser := CreateParser(test, "PromQL")
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
		parser := CreateParser(test, "PromQL")
		_, good := parser.parseMetric()

		if good {
			t.Fatalf("%s: Expected this to fail", test)
		}
	}
}

func TestParseMetricWithLabels(t *testing.T) {
	// <id>{<label_filter>}
	for _, test := range []string{
		"metric{label=\"value\"}",
		"metric {label=\"value\"}",
		"metric { label=\"value\"}",
		"metric { label=\"value\" }",
		"metric { label=\"value\"} ",
	} {
		parser := CreateParser(test, "PromQL")
		metric, good := parser.parseMetric()

		if !(metric.Name == "metric" && len(metric.LabelFilters) == 1) || !good {
			t.Fatalf("%s: Invalid metric got %s, %s", test, metric.Name, metric.LabelFilters)
		}
	}

	for _, test := range []string{
		"metric{label=\"value\", label2=\"zalue\"}",
		"metric {label=\"value\", label2=\"zalue\"  }",
		"metric { label=\"value\" ,label2=\"zalue\"}",
		"metric { label=\"value\" , label2=\"zalue\"}",
	} {
		parser := CreateParser(test, "PromQL")
		metric, good := parser.parseMetric()

		if !(metric.Name == "metric" && len(metric.LabelFilters) == 2) || !good {
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
		parser := CreateParser(test, "PromQL")
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
		parser := CreateParser(test, "PromQL")
		agg, good := parser.ParseAggregation()

		if !(agg.Op == "avg" && agg.Metric.Name == "metric" && len(agg.Metric.LabelFilters) == 0) || !good {
			t.Fatalf("%s: Invalid metric", test)
		}
	}

	for _, test := range []string{
		"avg(metric{label= \"test\"})",
		" avg(metric{label= \"test\"})",
		"avg( metric {label= \"test\"})",
	} {
		parser := CreateParser(test, "PromQL")
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
		parser := CreateParser(test, "PromQL")
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
		parser := CreateParser(test, "PromQL")
		agg, good := parser.ParseAggregation()

		if !(agg.Op == "avg" && agg.Metric.Name == "metric" && len(agg.By.Labels) == 2 && len(agg.Metric.LabelFilters) == 0) || !good {
			t.Fatalf("%s: Invalid metric", test)
		}
	}
}
