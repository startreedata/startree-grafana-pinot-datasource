package plugin

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPinotClient_Query(t *testing.T) {
	ctx := context.Background()
	client := mustCreateClient(t)

	sql := `SELECT "timestamp" AS 'time' FROM LogAnalyticsMonitoringwhere "timestamp" >= 1715881482102 AND "timestamp" <= 1715859882102`

	_, err := client.ExecuteSQL(ctx, "LogAnalyticsMonitoring", sql)
	assert.NoError(t, err)
}

func TestPinotClient_ListTables(t *testing.T) {
	ctx := context.Background()
	client := mustCreateClient(t)

	gotTables, err := client.ListTables(ctx)

	assert.NoError(t, err)
	assert.NotEmpty(t, gotTables)
}

func TestPinotClient_GetTableSchema(t *testing.T) {
	ctx := context.Background()
	client := mustCreateClient(t)

	res, err := client.GetTableSchema(ctx, "ABTestSampleData")

	assert.NoError(t, err)
	fmt.Printf("%v", res)
	assert.NotEmpty(t, res)
}

func mustCreateClient(t *testing.T) *PinotClient {
	client, err := NewPinotClient(PinotProperties{
		ControllerUrl: "https://pinot.demo.teprod.startree.cloud",
		BrokerUrl:     "https://broker.pinot.demo.teprod.startree.cloud",
		Authorization: "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjVhMWVmMWIwMTcyNDAwZDA2OGQxYTI5MDk3YWJiYmM2MGMzYjdlNmYifQ.eyJpc3MiOiJodHRwczovL2lkZW50aXR5LmRlbW8udGVwcm9kLnN0YXJ0cmVlLmNsb3VkIiwic3ViIjoiQ2lObmIyOW5iR1V0YjJGMWRHZ3lmREV4TVRZeU1Ea3lOVEkyTmpZMU5EUXpNakF6T1JJa1ltWXdZekE0TkdZdFpUTTNNQzAwTmpNMExXRXhOREV0Wm1JME4yWXlNekZoWXpBMSIsImF1ZCI6Im1hbmFnZWQtcGlub3QiLCJleHAiOjE3MTU4ODU4NjksImlhdCI6MTcxNTc5OTQ2OSwibm9uY2UiOiJyYW5kb21fc3RyaW5nIiwiYXRfaGFzaCI6InRwMWZuVHhFY2RudkVCSC0tOEhjQ2ciLCJlbWFpbCI6ImphY2tzb25Ac3RhcnRyZWUuYWkiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiZ3JvdXBzIjpbInN0YXJ0cmVlLW9wcy1kZWZhdWx0LXNyZSJdLCJuYW1lIjoiSmFja3NvbiBBcmdvIn0.J3_fRjhV79HaJVJH19dtYX3g0cAkaVYuqPhTiuelWEF1ijYcSOXMP4Ud-vWZLDp4TS1HMFSqrWsf9QMebXANJCaO7VBkyhV2RuDGIxVYFo5qmKEEIOzwmkxFSRGcJxiBmM9BYs1yXSVezJblLNKyr31wFtwaKLdjKkCnh5HdV1kd5MPV0zNBuknuXYeo0APZ9aAuPSigHJOgXPD3FJY37PunNVLDoi55FPv-toUc70PIQcb8yS2J8G-mNtoyt1ouGsvoIZ6R9t3SnLNMMEc7ZeTvdxzRG55AqrUoTMSINCkHivpnNO1V3Qn4FyKvWl0_upzYQPNHSgdu9MJqjmncRg",
	})
	require.NoError(t, err)
	return client
}
