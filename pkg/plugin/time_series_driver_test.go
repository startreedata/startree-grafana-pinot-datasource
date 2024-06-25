package plugin

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTimeSeriesAggTemplate(t *testing.T) {
	args := TimeSeriesTemplateArgs{
		TableName:           "my_table",
		DimensionColumns:    []string{"dim1", "dim2"},
		TimeColumn:          "ts",
		MetricColumn:        "met",
		AggregationFunction: "sum",
		TimeFilterExpr:      "where ts >= 10 and ts <= 20",
		TimeGroupExpr:       `DATETIMECONVERT(ts, '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MILLISECONDS')`,
		TimeColumnAlias:     TimeSeriesTimeColumnAlias,
		MetricColumnAlias:   TimeSeriesMetricColumnAlias,
	}
	var buf bytes.Buffer
	err := timeSeriesSqlTemplate.Execute(&buf, args)
	assert.NoError(t, err)
	fmt.Println(buf.String())
	assert.Equal(t, "", buf.String())
}
