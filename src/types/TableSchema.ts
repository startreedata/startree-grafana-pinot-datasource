export interface TableSchema {
  schemaName: string;
  dimensionFieldSpecs: DimensionFieldSpec[];
  metricFieldSpecs: MetricFieldSpec[];
  dateTimeFieldSpecs: DateTimeFieldSpec[];
}

export interface DimensionFieldSpec {
  name: string;
  dataType: string;
}

export interface MetricFieldSpec {
  name: string;
  dataType: string;
}

export interface DateTimeFieldSpec {
  name: string;
  dataType: string;
  format: string;
  granularity: string;
}
