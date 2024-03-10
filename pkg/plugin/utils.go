package plugin

import (
	"fmt"

	pinot "github.com/startreedata/pinot-client-go/pinot"
)

func getColumnIdx(col string, schema *pinot.RespSchema) (int, error) {
	for idx := 0; idx < len(schema.ColumnNames); idx++ {
		if schema.ColumnNames[idx] == col {
			return idx, nil
		}
	}

	return -1, fmt.Errorf("Column not found")
}
