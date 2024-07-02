import { DataSource } from '../datasource';
import { useEffect, useState } from 'react';
import { DateTime } from '@grafana/data';

export const PinotDataType = Object.freeze({
  INT: 'INT',
  LONG: 'LONG',
  FLOAT: 'FLOAT',
  DOUBLE: 'DOUBLE',
  STRING: 'STRING',
});

export const PinotDataTypes = Object.values(PinotDataType);
export const NumericPinotDataTypes = [PinotDataType.INT, PinotDataType.LONG, PinotDataType.FLOAT, PinotDataType.DOUBLE];

export interface GetDatabasesResponse {
  databases: string[];
}

export interface GetTablesResponse {
  tables: string[];
}

export interface GetTableSchemaResponse {
  schema: TableSchema;
}

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

interface SqlPreviewResponse {
  sql: string;
}

export interface SqlPreviewRequest {
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string;
  databaseName?: string;
  tableName?: string;
  timeColumn?: string;
  metricColumn?: string;
  groupByColumns?: string[];
  aggregationFunction?: string;
  filters?: DimensionFilter[];
}

export function useSqlPreview(datasource: DataSource, request: SqlPreviewRequest): string {
  const [data, setData] = useState<string>('');
  useEffect(() => {
    // TODO: No need to make the request until all fields are present.
    datasource.postResource<SqlPreviewResponse>('preview', request).then((resp) => setData(resp.sql));
  }, [JSON.stringify(request)]);
  return data;
}

interface DistinctValuesResponse {
  valueExprs: string[];
}

export interface DimensionFilter {
  columnName?: string;
  operator?: string;
  valueExprs?: string[];
}

export interface DistinctValuesRequest {
  databaseName?: string;
  tableName?: string;
  columnName?: string;
  timeColumn?: string;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  filters?: DimensionFilter[];
}

export function useDistinctValues(datasource: DataSource, request: DistinctValuesRequest): string[] | undefined {
  const [data, setData] = useState<string[]>([]);
  useEffect(() => {
    // TODO: No need to make the request until all fields are present.
    datasource.postResource<DistinctValuesResponse>('distinctValues', request).then((resp) => setData(resp.valueExprs));
  }, [JSON.stringify(request)]);
  return data;
}

export function useDatabases(datasource: DataSource): string[] | undefined {
  const resp = useControllerResource<GetDatabasesResponse>(datasource, undefined, 'databases');
  return resp?.databases;
}

export function useTables(datasource: DataSource, databaseName: string | undefined): string[] | undefined {
  const resp = useControllerResource<GetTablesResponse>(datasource, databaseName, 'tables');
  return resp?.tables;
}

export function useTableSchema(
  datasource: DataSource,
  databaseName: string | undefined,
  tableName: string | undefined
): TableSchema | undefined {
  const noop = !tableName;
  const resp = useControllerResource<GetTableSchemaResponse>(
    datasource,
    databaseName,
    'tables/' + tableName + '/schema',
    noop
  );
  return resp?.schema;
}

function useControllerResource<T>(
  datasource: DataSource,
  databaseName: string | undefined,
  path: string,
  noop?: boolean
): T | undefined {
  const [resp, setResp] = useState<T | undefined>(undefined);

  const params = new URLSearchParams();
  if (databaseName) {
    params.set('database', databaseName);
  }

  useEffect(() => {
    if (noop) return;
    datasource.getResource<T>(`${path}?${params.toString()}`).then((resp) => setResp(resp));
  }, [databaseName, path]);
  return resp;
}
