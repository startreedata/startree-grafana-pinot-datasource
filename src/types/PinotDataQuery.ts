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

export function interpolateVariables(query: PinotDataQuery, scopedVars: ScopedVars): PinotDataQuery {
  const templateSrv = getTemplateSrv();

  const replace = (target: string) => templateSrv.replace(target, scopedVars);
  const replaceIfExists = (target?: string | null) => (target ? replace(target) : undefined);

  return {
    ...query,

    // Sql Builder

    timeColumn: replaceIfExists(query.timeColumn),
    metricColumn: replaceIfExists(query.metricColumn),
    granularity: replaceIfExists(query.granularity),
    aggregationFunction: replaceIfExists(query.aggregationFunction),
    groupByColumns: query.groupByColumns?.map((columnName) => replace(columnName)),
    groupByColumnsV2: query.groupByColumnsV2?.map(({ name, key }) => ({
      name: replace(name),
      key: replaceIfExists(key),
    })),
    filters: query.filters?.map(({ columnName, operator, valueExprs }) => ({
      columnName,
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

    variableQuery: query.variableQuery
      ? {
          ...query.variableQuery,
          columnName: replaceIfExists(query.variableQuery.columnName),
          pinotQlCode: replaceIfExists(query.variableQuery.pinotQlCode),
        }
      : undefined,
  };
}
