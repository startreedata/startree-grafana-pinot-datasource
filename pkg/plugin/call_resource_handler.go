package plugin

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/resource/httpadapter"
	"github.com/startree/pinot/pkg/plugin/pinotlib"
	"github.com/startree/pinot/pkg/plugin/resources"
)

func NewCallResourceHandler(client *pinotlib.PinotClient) backend.CallResourceHandler {
	return httpadapter.New(resources.NewPinotResourceHandler(client))
}
