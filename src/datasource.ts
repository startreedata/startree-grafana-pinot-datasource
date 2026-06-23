import {
  AdHocVariableFilter,
  DataSourceGetTagKeysOptions,
  DataSourceGetTagValuesOptions,
  DataSourceInstanceSettings,
  MetricFindValue,
  ScopedVars,
} from '@grafana/data';
import { DataSourceWithBackend } from '@grafana/runtime';

import { interpolateVariables, PinotDataQuery } from './dataquery/PinotDataQuery';
import { PinotConnectionConfig } from './config/PinotConnectionConfig';
import { PinotVariableSupport } from './variables';
import { AnnotationsQueryEditor } from './components/AnnotationsQueryEditor/AnnotationsQueryEditor';
import { listColumns } from './resources/columns';
import { queryDistinctValuesForFilters } from './resources/distinctValues';

export class DataSource extends DataSourceWithBackend<PinotDataQuery, PinotConnectionConfig> {
  // Table context for ad-hoc filters. Pinot columns are per-table but Grafana's getTagValues
  // gives us only the column key, so we remember the table/time column resolved from the panel
  // queries in getTagKeys and reuse it for getTagValues.
  // ponytail: single cached context, good for single-table dashboards; revisit if a dashboard
  // mixes tables and needs per-key tables.
  private adHocContext?: { tableName: string; timeColumn?: string };

  constructor(instanceSettings: DataSourceInstanceSettings<PinotConnectionConfig>) {
    super(instanceSettings);

    this.variables = new PinotVariableSupport(this);
    this.annotations = { QueryEditor: AnnotationsQueryEditor };
  }

  applyTemplateVariables(
    query: PinotDataQuery,
    scopedVars: ScopedVars,
    filters?: AdHocVariableFilter[]
  ): PinotDataQuery {
    return interpolateVariables(query, scopedVars, filters);
  }

  interpolateVariablesInQueries(
    queries: PinotDataQuery[],
    scopedVars: ScopedVars,
    filters?: AdHocVariableFilter[]
  ): PinotDataQuery[] {
    return queries.map((query) => interpolateVariables(query, scopedVars, filters));
  }

  async getTagKeys(options?: DataSourceGetTagKeysOptions<PinotDataQuery>): Promise<MetricFindValue[]> {
    const context = this.resolveAdHocContext(options?.queries);
    if (!context) {
      return [];
    }
    const columns = await listColumns(this, { tableName: context.tableName });
    const names = new Set(columns.filter((column) => !column.isTime).map((column) => column.name));
    return Array.from(names, (name) => ({ text: name }));
  }

  async getTagValues(options: DataSourceGetTagValuesOptions): Promise<MetricFindValue[]> {
    const context = this.adHocContext;
    if (!context) {
      return [];
    }
    const values = await queryDistinctValuesForFilters(this, {
      tableName: context.tableName,
      columnName: options.key,
      timeColumn: context.timeColumn,
      timeRange: options.timeRange ? { from: options.timeRange.from, to: options.timeRange.to } : undefined,
    });
    return values.map((value) => ({ text: unquoteSqlLiteral(value) }));
  }

  private resolveAdHocContext(queries?: PinotDataQuery[]): { tableName: string; timeColumn?: string } | undefined {
    const query = queries?.find((q) => q.tableName);
    if (query?.tableName) {
      this.adHocContext = { tableName: query.tableName, timeColumn: query.timeColumn };
    }
    return this.adHocContext;
  }
}

// queryDistinctValuesForFilters returns quoted SQL value expressions (e.g. `'NewYork'`); the ad-hoc
// values dropdown needs the raw value, so undo the literal quoting/escaping. Non-string literals
// (numbers, booleans) are returned unchanged.
function unquoteSqlLiteral(expr: string): string {
  if (expr.length >= 2 && expr.startsWith("'") && expr.endsWith("'")) {
    return expr.slice(1, -1).replace(/''/g, "'");
  }
  return expr;
}
