package pinotlib

import (
	"context"
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib/pinottest"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPinotClient_ExecuteSqlQuery(t *testing.T) {
	ctx := context.Background()
	client := setupPinotAndCreateClient(t)
	query := NewSqlQuery(fmt.Sprintf(`select * from %s limit 10`, pinottest.InfraMetricsTableName))
	_, err := client.ExecuteSqlQuery(ctx, query)
	assert.NoError(t, err)
}
