package plugin

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDatasource(t *testing.T) {
	got, err := time.Parse(time.DateTime, "2014-01-01 00:00:00.0")
	assert.NoError(t, err)
	assert.Equal(t, got, time.Date(2014, 1, 1, 0, 0, 0, 0, time.UTC))
	fmt.Println(got)
}
