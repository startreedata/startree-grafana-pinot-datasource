import { QueryType } from './QueryType';
import { EditorMode } from './EditorMode';
import { DataQuery } from '@grafana/schema';
import { DimensionFilter } from '../resources/resources';

export const GetDefaultPinotDataQuery = (): Partial<PinotDataQuery> => ({
  queryType: QueryType.PinotQL,
  editorMode: EditorMode.Builder,
  databaseName: 'default',
});

export interface PinotDataQuery extends DataQuery {
  queryType?: string;
  editorMode?: string;
  databaseName?: string;
  tableName?: string;

  // PinotQl Builder
  timeColumn?: string;
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
          return !!(query.tableName && query.timeColumn && query.metricColumn && query.aggregationFunction);
        case EditorMode.Code:
          return !!query.pinotQlCode;
        default:
          return false;
      }
    default:
      return false;
  }
}
