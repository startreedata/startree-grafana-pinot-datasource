import { ComplexField } from '../dataquery/ComplexField';
import { DimensionFilter } from '../dataquery/DimensionFilter';
import { QueryOption } from '../dataquery/QueryOption';
import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { QueryType } from '../dataquery/QueryType';
import { EditorMode } from '../dataquery/EditorMode';
import { isEmpty } from 'lodash';
import { DisplayType } from '../dataquery/DisplayType';
import { Column, useColumns } from '../resources/columns';
import { DataSource } from '../datasource';
import { DateTime } from '@grafana/data';
import { useTables } from '../resources/tables';
import { UseResourceResult } from '../resources/UseResourceResult';
import { NumericPinotDataTypes, PinotDataType } from '../dataquery/PinotDataType';
import { useEffect, useState } from 'react';
import { previewTracesSql, PreviewTracesSqlRequest } from '../resources/previewSql';

export const DurationUnits = ['ns', 'µs', 'ms', 's'];
const DefaultDurationUnit = 'ms';

export interface Params {
  tableName: string;
  timeColumn: string;
  traceIdColumn: ComplexField;
  spanIdColumn: ComplexField;
  parentSpanIdColumn: ComplexField;
  serviceNameColumn: ComplexField;
  spanNameColumn: ComplexField;
  durationColumn: ComplexField;
  durationUnit: string;
  tagsColumn: ComplexField;
  statusColumn: ComplexField;
  traceId: string;
  limit: number;
  filters: DimensionFilter[];
  queryOptions: QueryOption[];
}

export interface Resources {
  tables: string[];
  isTablesLoading: boolean;
  columns: Column[];
  timeColumns: Column[];
  stringColumns: Column[];
  numericColumns: Column[];
  tagsColumns: Column[];
  filterColumns: Column[];
  isColumnsLoading: boolean;
  sqlPreview: string;
  isSqlPreviewLoading: boolean;
}

// Common OpenTelemetry column names, used to auto-map a span table's columns to the trace roles so
// the user rarely has to wire all of them by hand. Matched case-insensitively.
const ColumnNameHints: Record<keyof Pick<Params, 'traceIdColumn' | 'spanIdColumn' | 'parentSpanIdColumn' | 'serviceNameColumn' | 'spanNameColumn' | 'durationColumn' | 'tagsColumn' | 'statusColumn'>, string[]> = {
  traceIdColumn: ['traceid', 'trace_id'],
  spanIdColumn: ['spanid', 'span_id'],
  parentSpanIdColumn: ['parentspanid', 'parent_span_id'],
  serviceNameColumn: ['servicename', 'service_name', 'service'],
  spanNameColumn: ['name', 'operationname', 'operation_name', 'span_name'],
  durationColumn: ['durationms', 'duration_ms', 'durationnano', 'duration_nano', 'durationnanos', 'duration'],
  tagsColumn: ['tags', 'attributes', 'span_attributes', 'spanattributes'],
  statusColumn: ['statuscode', 'status_code', 'status'],
};

export function paramsFrom(query: PinotDataQuery): Params {
  return {
    tableName: query.tableName || '',
    timeColumn: query.timeColumn || '',
    traceIdColumn: query.traceIdColumn || {},
    spanIdColumn: query.spanIdColumn || {},
    parentSpanIdColumn: query.parentSpanIdColumn || {},
    serviceNameColumn: query.serviceNameColumn || {},
    spanNameColumn: query.spanNameColumn || {},
    durationColumn: query.durationColumn || {},
    durationUnit: query.durationUnit || DefaultDurationUnit,
    tagsColumn: query.tagsColumn || {},
    statusColumn: query.statusColumn || {},
    traceId: query.traceId || '',
    limit: query.limit || 0,
    filters: query.filters || [],
    queryOptions: query.queryOptions || [],
  };
}

export function canRunQuery(params: Params): boolean {
  switch (true) {
    case !params.tableName:
    case !params.timeColumn:
    case !params.traceIdColumn.name:
    case !params.spanIdColumn.name:
    case !params.durationColumn.name:
      return false;
    default:
      return true;
  }
}

// One-click preset that fills every trace role with the column name an OpenTelemetry-collector→Pinot
// table uses by default, so the user can point at an OTel span table and run without hand-picking each
// field. Unlike applyDefaults (which only fills unset roles from the live column list), this overwrites
// the column mappings unconditionally with the OTel semantic-convention names.
// ponytail: these names follow the OTel collector's Pinot exporter span schema; tune if your collector
// pipeline renames columns (e.g. flattens resource attributes differently).
export function otelDefaults(params: Params): Params {
  return {
    ...params,
    // ponytail: span trace/span identifiers per OTel span schema.
    traceIdColumn: { name: 'trace_id' },
    spanIdColumn: { name: 'span_id' },
    parentSpanIdColumn: { name: 'parent_span_id' },
    // ponytail: resource attribute service.name is typically flattened to a `service_name` column.
    serviceNameColumn: { name: 'service_name' },
    // ponytail: OTel calls the span's operation simply `name`.
    spanNameColumn: { name: 'name' },
    // ponytail: OTel records span duration in nanoseconds (duration_nano); pair with the 'ns' unit.
    durationColumn: { name: 'duration_nano' },
    durationUnit: 'ns',
    // ponytail: span attributes map/JSON column.
    tagsColumn: { name: 'span_attributes' },
    // ponytail: span status code column.
    statusColumn: { name: 'status_code' },
  };
}

export function applyDefaults(
  params: Params,
  resources: {
    timeColumns: Column[];
    columns: Column[];
  }
): boolean {
  let changed = false;

  const timeColumnCandidates = resources.timeColumns.filter((t) => !t.isDerived);
  if (!params.timeColumn && timeColumnCandidates.length > 0) {
    changed = true;
    params.timeColumn = timeColumnCandidates[0].name;
  }

  // Auto-map each unset role to the first column whose name matches a known OpenTelemetry hint.
  for (const role of Object.keys(ColumnNameHints) as Array<keyof typeof ColumnNameHints>) {
    if (params[role]?.name) {
      continue;
    }
    const hints = ColumnNameHints[role];
    const match = resources.columns.find((c) => hints.includes(c.name.toLowerCase()));
    if (match) {
      changed = true;
      params[role] = { name: match.name, key: match.key || undefined };
    }
  }
  return changed;
}

export function dataQueryOf(query: PinotDataQuery, params: Params): PinotDataQuery {
  const orUndefined = (field: ComplexField) => (field.name ? field : undefined);
  return {
    ...query,
    queryType: QueryType.PinotQL,
    editorMode: EditorMode.Builder,
    displayType: DisplayType.TRACES,
    tableName: params.tableName || undefined,
    timeColumn: params.timeColumn || undefined,
    traceIdColumn: orUndefined(params.traceIdColumn),
    spanIdColumn: orUndefined(params.spanIdColumn),
    parentSpanIdColumn: orUndefined(params.parentSpanIdColumn),
    serviceNameColumn: orUndefined(params.serviceNameColumn),
    spanNameColumn: orUndefined(params.spanNameColumn),
    durationColumn: orUndefined(params.durationColumn),
    durationUnit: params.durationUnit || undefined,
    tagsColumn: orUndefined(params.tagsColumn),
    statusColumn: orUndefined(params.statusColumn),
    traceId: params.traceId || undefined,
    limit: params.limit || undefined,
    filters: isEmpty(params.filters) ? undefined : params.filters,
    queryOptions: isEmpty(params.queryOptions) ? undefined : params.queryOptions,
  };
}

export function useResources(
  datasource: DataSource,
  timeRange: { to: DateTime | undefined; from: DateTime | undefined },
  interpolatedParams: Params
): Resources {
  const tablesResult = useTables(datasource);

  const columnsResult = useColumns(datasource, {
    timeRange: {
      to: timeRange.to?.endOf('second'),
      from: timeRange.from?.startOf('second'),
    },
    tableName: interpolatedParams.tableName,
    timeColumn: interpolatedParams.timeColumn,
    filters: interpolatedParams.filters,
  });

  const sqlPreviewResult = useSqlPreview(datasource, timeRange, interpolatedParams);
  return resourcesFrom(tablesResult, columnsResult, sqlPreviewResult);
}

export function resourcesFrom(
  tablesResult: UseResourceResult<string[]>,
  columnsResult: UseResourceResult<Column[]>,
  sqlPreviewResult: UseResourceResult<string>
): Resources {
  const { result: tables, loading: isTablesLoading } = tablesResult;
  const { result: columns, loading: isColumnsLoading } = columnsResult;
  const { result: sqlPreview, loading: isSqlPreviewLoading } = sqlPreviewResult;
  return {
    tables,
    isTablesLoading,
    columns,
    timeColumns: columns.filter(({ isTime, isDerived }) => isTime && !isDerived),
    stringColumns: columns.filter(({ dataType }) => dataType === PinotDataType.STRING),
    numericColumns: columns.filter(({ dataType }) => NumericPinotDataTypes.includes(dataType)),
    tagsColumns: columns.filter(({ dataType }) => [PinotDataType.JSON, PinotDataType.MAP].includes(dataType)),
    filterColumns: columns.filter(({ isTime }) => !isTime),
    isColumnsLoading,
    sqlPreview,
    isSqlPreviewLoading,
  };
}

function useSqlPreview(
  datasource: DataSource,
  timeRange: { to: DateTime | undefined; from: DateTime | undefined },
  interpolatedParams: Params
): UseResourceResult<string> {
  const [result, setResult] = useState('');
  const [loading, setLoading] = useState(false);

  const previewRequest: PreviewTracesSqlRequest = {
    timeRange: {
      to: timeRange.to?.endOf('second'),
      from: timeRange.from?.startOf('second'),
    },
    expandMacros: true,
    tableName: interpolatedParams.tableName,
    timeColumn: interpolatedParams.timeColumn,
    traceIdColumn: interpolatedParams.traceIdColumn,
    spanIdColumn: interpolatedParams.spanIdColumn,
    parentSpanIdColumn: interpolatedParams.parentSpanIdColumn,
    serviceNameColumn: interpolatedParams.serviceNameColumn,
    spanNameColumn: interpolatedParams.spanNameColumn,
    durationColumn: interpolatedParams.durationColumn,
    durationUnit: interpolatedParams.durationUnit,
    tagsColumn: interpolatedParams.tagsColumn,
    statusColumn: interpolatedParams.statusColumn,
    traceId: interpolatedParams.traceId,
    limit: interpolatedParams.limit,
    filters: interpolatedParams.filters,
    queryOptions: interpolatedParams.queryOptions,
  };

  useEffect(() => {
    setLoading(true);
    // Set the result unconditionally so an empty response (incomplete query) clears any stale SQL
    // instead of leaving the preview showing a previous valid configuration.
    previewTracesSql(datasource, previewRequest)
      .then((val) => setResult(val))
      .finally(() => setLoading(false));
  }, [datasource, JSON.stringify(previewRequest)]); // eslint-disable-line react-hooks/exhaustive-deps
  return { result, loading };
}
