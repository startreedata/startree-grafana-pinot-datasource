import { AdHocVariableFilter, CoreApp, DataSourceInstanceSettings, ScopedVars } from '@grafana/data';
import { DataSourceWithBackend } from '@grafana/runtime';

import { GetDefaultPinotDataQuery, interpolateVariables, PinotDataQuery } from './types/PinotDataQuery';
import { PinotConnectionConfig } from './types/PinotConnectionConfig';
import { PinotVariableSupport } from './variables';
import { QueryType } from './types/QueryType';
import { EditorMode } from './types/EditorMode';

export class DataSource extends DataSourceWithBackend<PinotDataQuery, PinotConnectionConfig> {
  constructor(instanceSettings: DataSourceInstanceSettings<PinotConnectionConfig>) {
    super(instanceSettings);

    this.variables = new PinotVariableSupport(this);
  }

  getDefaultQuery(_: CoreApp): Partial<PinotDataQuery> {
    return GetDefaultPinotDataQuery();
  }

  getQueryDisplayText(query: PinotDataQuery): string {
    switch (query.queryType) {
      case QueryType.PromQL:
        return query.promQlCode || 'Empty query';
      case QueryType.PinotQL:
        switch (query.editorMode) {
          case EditorMode.Code:
            return query.pinotQlCode || 'Empty query';
          case EditorMode.Builder:
            const filters =
              query.filters?.map((f) => `${f.columnName} ${f.operator} ${f.valueExprs?.join(',')}`).join(',') || 'none';
            const dims = query.groupByColumns?.join(',') || 'none';
            return `Table: ${query.tableName}, Time: ${query.timeColumn}, Aggregation: ${query.aggregationFunction}, Metric: ${query.metricColumn}, Dimensions: ${dims} Filters: ${filters}`;
          default:
            return 'Empty query';
        }
      default:
        return 'Empty query';
    }
  }

  applyTemplateVariables(
    query: PinotDataQuery,
    scopedVars: ScopedVars,
    filters?: AdHocVariableFilter[]
  ): PinotDataQuery {
    return interpolateVariables(query, scopedVars);
  }

  interpolateVariablesInQueries(
    queries: PinotDataQuery[],
    scopedVars: ScopedVars,
    filters?: AdHocVariableFilter[]
  ): PinotDataQuery[] {
    return queries.map((query) => interpolateVariables(query, scopedVars));
  }
}
