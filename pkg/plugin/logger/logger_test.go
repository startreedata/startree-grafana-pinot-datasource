package logger

import (
	"fmt"
	"testing"
)

func TestX(t *testing.T) {
	Error("Oopsie occurred", "error", fmt.Errorf("this is an error"))
	WithError(fmt.Errorf("this is an error")).Error("Oopsie occurred")
}
