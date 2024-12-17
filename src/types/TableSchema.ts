export interface TableSchema {
  schemaName: string;
  dimensionFieldSpecs: DimensionFieldSpec[] | null;
  metricFieldSpecs: MetricFieldSpec[] | null;
  dateTimeFieldSpecs: DateTimeFieldSpec[] | null;
  complexFieldSpecs: ComplexFieldSpec[] | null;
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

export interface ComplexFieldSpec {}
