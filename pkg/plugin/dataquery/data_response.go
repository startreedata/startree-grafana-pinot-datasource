package dataquery

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"net/http"
)

func NewEmptyDataResponse() backend.DataResponse {
	return backend.DataResponse{Status: http.StatusOK}
}

func NewSqlQueryDataResponse(frame *data.Frame, exceptions []pinot.BrokerException) backend.DataResponse {
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

func NewPartialDataResponse(frames []*data.Frame, exceptions []pinot.BrokerException) backend.DataResponse {
	return backend.DataResponse{
		Status: backend.StatusInternal,
		Frames: frames,
		Error:  backend.DownstreamError(pinot.NewBrokerExceptionError(exceptions)),
	}
}

func NewPinotExceptionsDataResponse(exceptions []pinot.BrokerException) backend.DataResponse {
	return backend.DataResponse{
		Status: backend.StatusInternal,
		Error:  backend.DownstreamError(pinot.NewBrokerExceptionError(exceptions)),
	}
}

func NewBadRequestErrorResponse(err error) backend.DataResponse {
	return NewErrorDataResponse(backend.StatusBadRequest, err)
}

func NewPluginErrorResponse(err error) backend.DataResponse {
	return NewInternalErrorDataResponse(err)
}

func NewDownstreamErrorResponse(err error) backend.DataResponse {
	return NewInternalErrorDataResponse(backend.DownstreamError(err))
}

func NewInternalErrorDataResponse(err error) backend.DataResponse {
	return NewErrorDataResponse(backend.StatusInternal, err)
}

func NewErrorDataResponse(status backend.Status, err error) backend.DataResponse {
	return backend.DataResponse{
		Status: status,
		Error:  err,
	}
}

// WithWarningNotices attaches each message as a warning notice on the response's frames, which
// Grafana shows as a warning icon in the panel header.
func WithWarningNotices(resp backend.DataResponse, messages []string) backend.DataResponse {
	if len(messages) == 0 || len(resp.Frames) == 0 {
		return resp
	}
	frame := resp.Frames[0]
	if frame.Meta == nil {
		frame.Meta = &data.FrameMeta{}
	}
	for _, msg := range messages {
		frame.Meta.Notices = append(frame.Meta.Notices, data.Notice{Severity: data.NoticeSeverityWarning, Text: msg})
	}
	return resp
}
