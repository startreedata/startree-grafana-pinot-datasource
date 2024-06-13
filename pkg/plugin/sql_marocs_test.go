package plugin

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestExpandMacros_TableName(t *testing.T) {
	queryContext := QueryContext{
		TableName:  "my_table",
		SqlContext: SqlContext{RawSql: "SELECT * FROM __tableName"},
	}

	res, err := ExpandMacros(queryContext, "SELECT * FROM __tableName")
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM  my_table", res)
}

func TestExpandMacros_TimeFilterMacro(t *testing.T) {
	queryContext := QueryContext{
		TableName: "my_table",
		TableSchema: TableSchema{
			SchemaName: "my_table",
			DimensionFieldSpecs: []DimensionFieldSpec{{
				Name:     "dim",
				DataType: "STRING",
			}},
			MetricFieldSpecs: []MetricFieldSpec{{
				Name:     "met",
				DataType: "DOUBLE",
			}},
			DateTimeFieldSpecs: []DateTimeFieldSpec{{
				Name:        "ts",
				DataType:    "LONG",
				Format:      "EPOCH|MILLISECONDS|1",
				Granularity: "MILLISECONDS|1",
			}},
		},
		IntervalSize: 15000000000,
		TimeRange: backend.TimeRange{
			From: time.Date(2019, time.May, 16, 18, 1, 12, 642000000, time.Local),
			To:   time.Date(2024, time.May, 16, 18, 1, 12, 642000000, time.Local),
		},
		SqlContext: SqlContext{RawSql: `SELECT * FROM my_table WHERE __timeFilter("ts")`},
	}

	res, err := ExpandMacros(queryContext, `SELECT * FROM my_table WHERE __timeFilter("ts")`)
	assert.NoError(t, err)
	assert.Equal(t, `SELECT * FROM my_table WHERE  "ts" >= 1558044072642 AND "ts" <= 1715896872642`, res)
}
