package plugin

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDisposerFunc_Dispose(t *testing.T) {
	var disposed bool
	disposerFunc(func() { disposed = true }).Dispose()
	assert.True(t, disposed)
}
