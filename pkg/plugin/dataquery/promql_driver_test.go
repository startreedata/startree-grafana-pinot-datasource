package dataquery

import (
	"context"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPromQlDriver_Execute(t *testing.T) {
	client := test_helpers.SetupPinotAndCreateClient(t)
	driver := NewPromQlCodeDriver(PromQlCodeDriverParams{
		PinotClient: client,
		TableName:   "infraMetrics",
		PromQlCode:  "sum(db_record_write)",
		TimeRange: TimeRange{
			From: time.Unix(1726617600, 0),
			To:   time.Unix(1726619400, 0),
		},
		IntervalSize: 60 * time.Second,
		Legend:       "",
	})

	resp := driver.Execute(context.Background())
	assert.NoError(t, resp.Error)
	assert.NotEmpty(t, resp.Frames)
}
