import {DataQuery} from '@grafana/schema';
import {BuilderMode, QueryBuilderOptions, QueryType} from './queryBuilder';

/**
 * EditorType determines the query editor type.
 */
export enum EditorType {
  SQL = 'sql',
  Builder = 'builder',
}

export interface PinotQueryBase extends DataQuery {
  pluginVersion: string,
  editorType: EditorType;
  rawSql: string;
  tableName?: string;
  queryType?: string;


  timeColumn?: string;
  metricColumn?: string;
  dimensionColumns?: string[];
  aggregationFunction?: string;
}

export interface PinotSqlQuery extends PinotQueryBase {
  editorType: EditorType.SQL;
  meta?: {
    timezone?: string;
    // meta fields to be used just for building builder options when migrating back to EditorType.Builder
    builderOptions?: QueryBuilderOptions;
  };
  expand?: boolean;
}

export interface PinotBuilderQuery extends PinotQueryBase {
  editorType: EditorType.Builder;
  builderOptions: QueryBuilderOptions;
  meta?: {
    timezone?: string;
  };
}

export type PinotQuery = PinotSqlQuery | PinotBuilderQuery;


// TODO: these aren't really types
export const defaultEditorType: EditorType = EditorType.Builder;
export const defaultCHBuilderQuery: Omit<PinotBuilderQuery, 'refId'> = {
  pluginVersion: '',
  editorType: EditorType.Builder,
  rawSql: '',
  builderOptions: {
    database: '',
    table: '',
    queryType: QueryType.Table,
    mode: BuilderMode.List,
    columns: [],
    meta: {},
    limit: 1000
  },
};
export const defaultCHSqlQuery: Omit<PinotSqlQuery, 'refId'> = {
  pluginVersion: '',
  editorType: EditorType.SQL,
  rawSql: '',
  expand: false,
};


export const DEFAULT_QUERY: Partial<PinotQuery> = {
  pluginVersion: '0.0.0',
  editorType: EditorType.SQL,
  rawSql: '',
  tableName: '',
  queryType: 'PinotSQL',
  meta: {
    timezone: 'UTC',
  },
};
