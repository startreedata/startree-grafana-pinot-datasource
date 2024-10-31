package dataquery

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"net/http"
)

func NewEmptyDataResponse() backend.DataResponse {
	return backend.DataResponse{Status: http.StatusOK}
}

func NewSqlQueryDataResponse(frame *data.Frame, exceptions []pinotlib.BrokerException) backend.DataResponse {
	if len(exceptions) == 0 {
		return NewOkDataResponse(frame)
	} else {
		return NewPartialDataResponse([]*data.Frame{frame}, exceptions)
	}
}

func NewOkDataResponse(frames ...*data.Frame) backend.DataResponse {
	return backend.DataResponse{
		Status: backend.StatusOK,
		Frames: frames,
	}
}

func NewPartialDataResponse(frames []*data.Frame, exceptions []pinotlib.BrokerException) backend.DataResponse {
	return backend.DataResponse{
		Status:      backend.StatusInternal,
		Frames:      frames,
		Error:       pinotlib.NewBrokerExceptionError(exceptions),
		ErrorSource: backend.ErrorSourceDownstream,
	}
}

func NewPinotExceptionsDataResponse(exceptions []pinotlib.BrokerException) backend.DataResponse {
	return backend.DataResponse{
		Status:      backend.StatusInternal,
		Error:       pinotlib.NewBrokerExceptionError(exceptions),
		ErrorSource: backend.ErrorSourceDownstream,
	}
}

func NewPluginErrorResponse(err error) backend.DataResponse {
	return NewInternalErrorDataResponse(err, backend.ErrorSourcePlugin)
}

func NewDownstreamErrorResponse(err error) backend.DataResponse {
	return NewInternalErrorDataResponse(err, backend.ErrorSourceDownstream)
}

func NewInternalErrorDataResponse(err error, source backend.ErrorSource) backend.DataResponse {
	return NewErrorDataResponse(backend.StatusInternal, err, source)
}

func NewErrorDataResponse(status backend.Status, err error, source backend.ErrorSource) backend.DataResponse {
	return backend.DataResponse{
		Status:      status,
		Error:       err,
		ErrorSource: source,
	}
}
