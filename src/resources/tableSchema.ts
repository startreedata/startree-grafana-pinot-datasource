import { DataSource } from '../datasource';
import { useEffect, useState } from 'react';
import { PinotResourceResponse } from './PinotResourceResponse';
import { UseResourceResult } from './UseResourceResult';

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

export interface ComplexFieldSpec {
  name: string;
  dataType: string;
  fieldType: string;
  notNull: boolean;
  childFieldSpecs: ChildFieldSpecs;
}

export interface ChildFieldSpecs {
  key: ChildFieldSpec;
  value: ChildFieldSpec;
}

export interface ChildFieldSpec {
  name: string;
  dataType: string;
  fieldType: string;
  notNull: boolean;
}

export function useTableSchema(datasource: DataSource, tableName: string): UseResourceResult<TableSchema | null> {
  const [result, setResult] = useState<TableSchema | null>(null);
  const [loading, setLoading] = useState<boolean>(false);

  useEffect(() => {
    if (tableName) {
      setLoading(true);
      fetchTableSchema(datasource, tableName)
        .then((schema) => setResult(schema))
        .finally(() => setLoading(false));
    }
  }, [datasource, tableName]);

  return { loading, result };
}

export async function fetchTableSchema(datasource: DataSource, tableName: string): Promise<TableSchema | null> {
  const endpoint = 'tables/' + tableName + '/schema';
  type GetTableSchemaResponse = PinotResourceResponse<TableSchema>;
  return datasource.getResource<GetTableSchemaResponse>(endpoint).then((resp) => resp.result);
}
