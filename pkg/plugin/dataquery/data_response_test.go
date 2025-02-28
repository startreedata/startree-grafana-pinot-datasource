package dataquery

import (
	"errors"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewEmptyDataResponse(t *testing.T) {
	got := NewEmptyDataResponse()
	assert.Equal(t, backend.StatusOK, got.Status)
	assert.Empty(t, got.Frames)
	assert.Empty(t, got.Error)
	assert.Empty(t, got.ErrorSource)
}

func TestNewSqlQueryDataResponse(t *testing.T) {
	frame := data.NewFrame("test")
	t.Run("success", func(t *testing.T) {
		got := NewSqlQueryDataResponse(frame, nil)
		assert.Equal(t, backend.StatusOK, got.Status)
		assert.Equal(t, data.Frames{frame}, got.Frames)
		assert.Empty(t, got.Error)
		assert.Empty(t, got.ErrorSource)
	})

	t.Run("partial", func(t *testing.T) {
		exceptions := []pinot.BrokerException{{Message: "error", ErrorCode: 1}}
		got := NewSqlQueryDataResponse(frame, exceptions)
		assert.Equal(t, backend.StatusInternal, got.Status)
		assert.Equal(t, data.Frames{frame}, got.Frames)
		assert.Equal(t, pinot.NewBrokerExceptionError(exceptions), got.Error)
		assert.Equal(t, backend.ErrorSourceDownstream, got.ErrorSource)
	})
}

func TestNewOkDataResponse(t *testing.T) {
	frame := data.NewFrame("test")
	got := NewOkDataResponse(frame)
	assert.Equal(t, backend.StatusOK, got.Status)
	assert.Equal(t, data.Frames{frame}, got.Frames)
	assert.Empty(t, got.Error)
	assert.Empty(t, got.ErrorSource)
}

func TestNewPartialDataResponse(t *testing.T) {
	frame := data.NewFrame("test")
	exceptions := []pinot.BrokerException{{Message: "error", ErrorCode: 1}}
	got := NewSqlQueryDataResponse(frame, exceptions)
	assert.Equal(t, backend.StatusInternal, got.Status)
	assert.Equal(t, data.Frames{frame}, got.Frames)
	assert.Equal(t, pinot.NewBrokerExceptionError(exceptions), got.Error)
	assert.Equal(t, backend.ErrorSourceDownstream, got.ErrorSource)
}

func TestNewPinotExceptionsDataResponse(t *testing.T) {
	exceptions := []pinot.BrokerException{{Message: "error", ErrorCode: 1}}
	got := NewPinotExceptionsDataResponse(exceptions)
	assert.Equal(t, backend.StatusInternal, got.Status)
	assert.Empty(t, got.Frames)
	assert.Equal(t, pinot.NewBrokerExceptionError(exceptions), got.Error)
	assert.Equal(t, backend.ErrorSourceDownstream, got.ErrorSource)
}

func TestNewPluginErrorResponse(t *testing.T) {
	got := NewPluginErrorResponse(errors.New("error"))
	assert.Equal(t, backend.StatusInternal, got.Status)
	assert.Empty(t, got.Frames)
	assert.Equal(t, errors.New("error"), got.Error)
	assert.Equal(t, backend.ErrorSourcePlugin, got.ErrorSource)
}

func TestNewDownstreamErrorResponse(t *testing.T) {
	got := NewDownstreamErrorResponse(errors.New("error"))
	assert.Equal(t, backend.StatusInternal, got.Status)
	assert.Empty(t, got.Frames)
	assert.Equal(t, errors.New("error"), got.Error)
	assert.Equal(t, backend.ErrorSourceDownstream, got.ErrorSource)
}

func TestNewInternalErrorDataResponse(t *testing.T) {
	got := NewInternalErrorDataResponse(errors.New("error"), "error-source")
	assert.Equal(t, backend.StatusInternal, got.Status)
	assert.Empty(t, got.Frames)
	assert.Equal(t, errors.New("error"), got.Error)
	assert.Equal(t, backend.ErrorSource("error-source"), got.ErrorSource)
}

func TestNewErrorDataResponse(t *testing.T) {
	got := NewErrorDataResponse(100, errors.New("error"), "error-source")
	assert.Equal(t, backend.Status(100), got.Status)
	assert.Empty(t, got.Frames)
	assert.Equal(t, errors.New("error"), got.Error)
	assert.Equal(t, backend.ErrorSource("error-source"), got.ErrorSource)
}
