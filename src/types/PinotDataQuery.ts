import { QueryType } from './QueryType';
import { EditorMode } from './EditorMode';
import { DataQuery } from '@grafana/schema';
import { DimensionFilter } from './DimensionFilter';
import { OrderByClause } from './OrderByClause';
import { QueryOption } from './QueryOption';
import { getTemplateSrv } from '@grafana/runtime';
import { ScopedVars } from '@grafana/data';
import { PinotVariableQuery } from './PinotVariableQuery'; // TODO: It's not entirely clear to me how these defaults are populated.

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

  // PinotQl Code
  pinotQlCode?: string;
  timeColumnAlias?: string;
  timeColumnFormat?: string;
  metricColumnAlias?: string;
  displayType?: string;

  // Pinot Variable Query
  variableQuery?: PinotVariableQuery;
}

export function interpolateVariables(query: PinotDataQuery, scopedVars: ScopedVars): PinotDataQuery {
  const templateSrv = getTemplateSrv();
  return {
    ...query,

    // Sql Builder

    timeColumn: templateSrv.replace(query.timeColumn, scopedVars),
    metricColumn: templateSrv.replace(query.metricColumn, scopedVars),
    granularity: templateSrv.replace(query.granularity, scopedVars),
    aggregationFunction: templateSrv.replace(query.aggregationFunction, scopedVars),
    groupByColumns: (query.groupByColumns || []).map((columnName) => templateSrv.replace(columnName, scopedVars)),
    filters: (query.filters || []).map(({ columnName, operator, valueExprs }) => ({
      columnName,
      operator,
      valueExprs: valueExprs?.map((expr) => templateSrv.replace(expr, scopedVars)),
    })),
    queryOptions: (query.queryOptions || []).map(({ name, value }) => ({
      name,
      value: templateSrv.replace(value, scopedVars),
    })),

    // Sql Editor

    pinotQlCode: templateSrv.replace(query.pinotQlCode, scopedVars),
  };
}
