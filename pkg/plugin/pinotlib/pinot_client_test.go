package pinotlib

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPinotClient_Query(t *testing.T) {
	ctx := context.Background()
	client := NewPinotTestClient(t)
	sql := `select * from githubEvents limit 10`
	_, err := client.ExecuteSQL(ctx, "githubEvents", sql)
	assert.NoError(t, err)
}

func NewPinotTestClient(t *testing.T) *PinotClient {
	pinotClient, err := NewPinotClient(PinotClientProperties{
		ControllerUrl: "http://localhost:9000",
		BrokerUrl:     "http://localhost:8000",
	})
	require.NoError(t, err)
	return pinotClient
}
