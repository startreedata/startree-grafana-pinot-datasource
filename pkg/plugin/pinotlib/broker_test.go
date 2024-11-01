package pinotlib

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPinotClient_ExecuteSqlQuery(t *testing.T) {
	ctx := context.Background()
	client := setupPinotAndCreateClient(t)
	query := NewSqlQuery(`
		SELECT fabric, "pattern", ts, value
		FROM benchmark
		ORDER BY ts, fabric, "pattern", value
		LIMIT 3`)
	resp, err := client.ExecuteSqlQuery(ctx, query)
	assert.NoError(t, err)
	assert.Empty(t, resp.Exceptions)
	assert.Equal(t, &ResultTable{
		DataSchema: DataSchema{
			ColumnDataTypes: []string{"STRING", "STRING", "TIMESTAMP", "DOUBLE"},
			ColumnNames:     []string{"fabric", "pattern", "ts", "value"}},
		Rows: [][]interface{}{
			{"fabric_0000", "pattern_0001", "2024-10-01 00:00:00.0", json.Number("-1.037174743344011")},
			{"fabric_0000", "pattern_0011", "2024-10-01 00:00:00.0", json.Number("101.49030354351736")},
			{"fabric_0000", "pattern_0012", "2024-10-01 00:00:00.0", json.Number("201.0248989609479")},
		},
	}, resp.ResultTable)
}
