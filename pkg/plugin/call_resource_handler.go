package plugin

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/resource/httpadapter"
)


func NewCallResourceHandler(client *PinotClient) backend.CallResourceHandler {
	return httpadapter.New(NewPinotResourceHandler(client))
}
