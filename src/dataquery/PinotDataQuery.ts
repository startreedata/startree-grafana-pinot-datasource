import { QueryType } from './QueryType';
import { EditorMode } from './EditorMode';
import { DataQuery } from '@grafana/schema';
import { DimensionFilter } from './DimensionFilter';
import { OrderByClause } from './OrderByClause';
import { QueryOption } from './QueryOption';
import { getTemplateSrv } from '@grafana/runtime';
import { ScopedVars } from '@grafana/data';
import { PinotVariableQuery } from './PinotVariableQuery';
import { ComplexField } from './ComplexField'; // TODO: It's not entirely clear to me how these defaults are populated.

// TODO: It's not entirely clear to me how these defaults are populated.
export const GetDefaultPinotDataQuery = (): Partial<PinotDataQuery> => ({
  queryType: QueryType.PinotQL,
  editorMode: EditorMode.Builder,

  // PinotQl Builder

  limit: -1,

  // PinotQl Code Editor

  timeColumnAlias: 'time',
  metricColumnAlias: 'metric',
  timeColumnFormat: '1:MILLISECONDS:EPOCH',
  pinotQlCode: `
SELECT 
  $__timeGroup("timestamp") AS $__timeAlias(),
  SUM("metric") AS $__metricAlias()
FROM $__table()
WHERE $__timeFilter("timestamp")
GROUP BY $__timeGroup("timestamp")
ORDER BY $__timeAlias() DESC
LIMIT 100000
`.trim(),
});

// PinotDataQuery serves as both the saved data model and data query API model.
// And it's also overloaded as the primary data model that powers the ui components. 😬
// Since this is also the saved data model, we have to be careful to maintain backwards compability.
// Grafana does provide a data migration js api, however it's not available for Grafana 10.
// Ref https://grafana.com/developers/plugin-tools/how-to-guides/data-source-plugins/add-migration-handler-for-backend-data-source.
// TODO: Make unit-testable data conversions and data models for UI components instead of re-using this data model.
export interface PinotDataQuery extends DataQuery {
  queryType?: string;
  editorMode?: string;
  tableName?: string;

  // PinotQl Builder
  timeColumn?: string;
  granularity?: string;
  metricColumn?: string;
  groupByColumns?: string[];
  aggregationFunction?: string;
  limit?: number;
  filters?: DimensionFilter[];
  orderBy?: OrderByClause[];
  queryOptions?: QueryOption[];
  legend?: string;
  metricColumnV2?: ComplexField;
  groupByColumnsV2?: ComplexField[];

  // PinotQl Code
  pinotQlCode?: string;
  timeColumnAlias?: string;
  timeColumnFormat?: string;
  metricColumnAlias?: string;
  logColumnAlias?: string;
  displayType?: string;

  // Pinot Variable Query
  variableQuery?: PinotVariableQuery;

  // PromQl
  promQlCode?: string;
}

export function builderGroupByColumnsFrom(query: PinotDataQuery): ComplexField[] {
  return [...(query.groupByColumns?.map((col) => ({ name: col })) || []), ...(query.groupByColumnsV2 || [])];
}

export function builderMetricColumnFrom(query: PinotDataQuery): ComplexField | undefined {
  if (query.metricColumnV2) {
    return query.metricColumnV2;
  } else if (query.metricColumn) {
    return { name: query.metricColumn };
  } else {
    return undefined;
  }
}

export function interpolateVariables(query: PinotDataQuery, scopedVars: ScopedVars | undefined): PinotDataQuery {
  const templateSrv = getTemplateSrv();

  function mapIfExists<T>(target: T | undefined, mapper: (val: T) => T): T | undefined {
    return target ? mapper(target) : undefined;
  }

  const replace = (target: string) => templateSrv.replace(target, scopedVars);
  const replaceIfExists = (target?: string | null) => (target ? replace(target) : undefined);

  return {
    ...query,

    // Sql Builder

    timeColumn: replaceIfExists(query.timeColumn),
    metricColumn: replaceIfExists(query.metricColumn),
    metricColumnV2: mapIfExists(query.metricColumnV2, ({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),
    granularity: replaceIfExists(query.granularity),
    aggregationFunction: replaceIfExists(query.aggregationFunction),
    groupByColumns: query.groupByColumns?.map((columnName) => replace(columnName)),
    groupByColumnsV2: query.groupByColumnsV2?.map(({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),
    filters: query.filters?.map(({ columnName, columnKey, operator, valueExprs }) => ({
      columnName: replaceIfExists(columnName),
      columnKey: replaceIfExists(columnKey),
      operator,
      valueExprs: valueExprs?.map((expr) => replace(expr)),
    })),
    queryOptions: (query.queryOptions || []).map(({ name, value }) => ({
      name,
      value: replaceIfExists(value),
    })),

    // Sql Editor

    pinotQlCode: replaceIfExists(query.pinotQlCode),

    // PromQl Editor

    promQlCode: replaceIfExists(query.promQlCode),

    // Variable Query editor

    variableQuery: mapIfExists(query.variableQuery, (variableQuery) => ({
      ...variableQuery,
      columnName: replaceIfExists(variableQuery.columnName),
      pinotQlCode: replaceIfExists(variableQuery.pinotQlCode),
    })),
  };
}
