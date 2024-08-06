import { QueryType } from './QueryType';
import { EditorMode } from './EditorMode';
import { DataQuery } from '@grafana/schema';
import { DimensionFilter } from './DimensionFilter';
import { OrderByClause } from './OrderByClause';

// TODO: It's not entirely clear to me how these defaults are populated.
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
LIMIT 100000
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
  orderBy?: OrderByClause[];

  // PinotQl Code
  pinotQlCode?: string;
  timeColumnAlias?: string;
  timeColumnFormat?: string;
  metricColumnAlias?: string;
  displayType?: string;
}
