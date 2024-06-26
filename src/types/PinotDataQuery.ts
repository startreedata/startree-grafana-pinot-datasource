import { QueryType } from './QueryType';
import { EditorMode } from './EditorMode';
import { DataQuery } from '@grafana/schema';

export const GetDefaultPinotDataQuery = (): Partial<PinotDataQuery> => ({
  queryType: QueryType.PinotQL,
  editorMode: EditorMode.Builder,
  databaseName: 'default',
});

export interface PinotDataQuery extends DataQuery {
  queryType?: string;
  editorMode?: string;
  pinotQlCode?: string;
  databaseName?: string;
  tableName?: string;
  timeColumn?: string;
  metricColumn?: string;
  dimensionColumns?: string[];
  aggregationFunction?: string;
  limit?: number;
}
