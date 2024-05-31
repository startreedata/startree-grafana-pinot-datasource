package plugin

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTimeSeriesDriver(t *testing.T) {
	args := templArgs{
		TableName:        "my_table",
		DimensionColumns: []string{"dim1", "dim2"},
		TimeColumn:       "ts",
		MetricColumn:     "met",
		TimeFilterExp:    "where ts >= 10 and ts <= 20",
		TimeGroupExp:     `DATETIMECONVERT(ts, '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MILLISECONDS')`,
	}
	var buf bytes.Buffer
	err := timeSeriesSqlTemplate.Execute(&buf, args)
	assert.NoError(t, err)
	fmt.Println(buf.String())
	assert.Equal(t, "", buf.String())
}
