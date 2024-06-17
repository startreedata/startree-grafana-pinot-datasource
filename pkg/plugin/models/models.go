package models

type PinotQuery struct {
	QueryType           string          `json:"editorType"`
	TableName           string          `json:"tableName"`
	Fill                bool            `json:"fill"`
	FillInterval        float64         `json:"fillInterval"`
	FillMode            string          `json:"fillMode"`
	FillValue           float64         `json:"fillValue"`
	Format              string          `json:"format"`
	RawSql              string          `json:"rawSql"`
	TimeColumn          string          `json:"timeColumn"`
	MetricColumn        string          `json:"metricColumn"`
	DimensionColumns    []DimensionData `json:"dimensionColumns"`
	AggregationFunction string          `json:"aggregationFunction"`
}
