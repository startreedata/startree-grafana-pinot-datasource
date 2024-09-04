package plugin

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"regexp"
	"testing"
)

func TestDatasource(t *testing.T) {
	got := formatString(t, "{{city }}, {{ state}} {{ dim.My_Val&&*##@name }}", map[string]string{
		"city":                 "Albany",
		"state":                "New York",
		"dim.My_Val&&*##@name": "is very nice",
	})

	assert.Equal(t, "Albany, New York is very nice", got)
}

func formatString(t *testing.T, legend string, data map[string]string) string {
	for key := range data {
		pattern := fmt.Sprintf(`\{\{\s*%s\s*}}`, regexp.QuoteMeta(key))
		r, err := regexp.Compile(pattern)
		require.NoError(t, err)
		legend = r.ReplaceAllString(legend, data[key])
	}
	return legend
}
