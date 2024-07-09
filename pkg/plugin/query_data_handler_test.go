package plugin

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"testing"
)

func TestQueryData(t *testing.T) {
	client := testPinotClient(t)

	handler := NewQueryDataHandler(client)
	resp, err := handler.QueryData(
		context.Background(),
		&backend.QueryDataRequest{
			Queries: []backend.DataQuery{
				{RefID: "A"},
			},
		},
	)
	if err != nil {
		t.Error(err)
	}

	if len(resp.Responses) != 1 {
		t.Fatal("QueryData must return a response")
	}
}
