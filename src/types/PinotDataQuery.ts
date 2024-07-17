import { QueryType } from './QueryType';
import { EditorMode } from './EditorMode';
import { DataQuery } from '@grafana/schema';
import { DimensionFilter } from './DimensionFilter';

export const GetDefaultPinotDataQuery = (): Partial<PinotDataQuery> => ({
  queryType: QueryType.PinotQL,
  editorMode: EditorMode.Builder,
  databaseName: 'default',

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
LIMIT 1000000
`.trim(),
});

export interface PinotDataQuery extends DataQuery {
  queryType?: string;
  editorMode?: string;
  databaseName?: string;
  tableName?: string;

  // PinotQl Builder
  timeColumn?: string;
  granularity?: string;
  metricColumn?: string;
  groupByColumns?: string[];
  aggregationFunction?: string;
  limit?: number;
  filters?: DimensionFilter[];

  // PinotQl Code
  pinotQlCode?: string;
  timeColumnAlias?: string;
  timeColumnFormat?: string;
  metricColumnAlias?: string;
}

export function canRunQuery(query: PinotDataQuery): boolean {
  switch (query.queryType) {
    case QueryType.PinotQL:
      switch (query.editorMode) {
        case EditorMode.Builder:
          return !!(
            query.tableName &&
            query.timeColumn &&
            query.aggregationFunction &&
            (query.metricColumn || query.aggregationFunction === 'COUNT')
          );
        case EditorMode.Code:
          return !!query.pinotQlCode;
        default:
          return false;
      }
    default:
      return false;
  }
}
