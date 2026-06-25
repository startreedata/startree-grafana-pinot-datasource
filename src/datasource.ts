import {
  AdHocVariableFilter,
  DataSourceGetTagKeysOptions,
  DataSourceGetTagValuesOptions,
  DataSourceInstanceSettings,
  DateTime,
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
    const context = this.resolveAdHocContext(readAdHocRequest(options).queries);
    if (!context) {
      return [];
    }
    const columns = await listColumns(this, { tableName: context.tableName });
    const names = new Set(columns.filter((column) => !column.isTime).map((column) => column.name));
    return Array.from(names, (name) => ({ text: name }));
  }

  async getTagValues(options: DataSourceGetTagValuesOptions): Promise<MetricFindValue[]> {
    const context = this.adHocContext;
    const { timeRange } = readAdHocRequest(options);
    // The distinct-values backend requires a valid time column whenever a time range is sent, so
    // only query when both are known; otherwise short-circuit instead of triggering a 500.
    if (!context || !context.timeColumn || !timeRange) {
      return [];
    }
    const values = await queryDistinctValuesForFilters(this, {
      tableName: context.tableName,
      columnName: options.key,
      timeColumn: context.timeColumn,
      timeRange: { from: timeRange.from, to: timeRange.to },
    });
    return values.map((value) => ({ text: unquoteSqlLiteral(value) }));
  }

  private resolveAdHocContext(queries?: PinotDataQuery[]): { tableName: string; timeColumn?: string } | undefined {
    // Clear the cache when no table can be resolved so stale context doesn't leak across
    // dashboards/panels (e.g. a request with no queries).
    const query = queries?.find((q) => q.tableName);
    this.adHocContext = query?.tableName ? { tableName: query.tableName, timeColumn: query.timeColumn } : undefined;
    return this.adHocContext;
  }
}

// `queries` (getTagKeys) and `timeRange` (getTagValues) are best-effort context that some Grafana
// versions drop. Reading them through this local type instead of the @grafana/data option types
// keeps them out of the plugin-compatibility (levitate) check — when absent, tag keys/values just
// degrade to empty rather than breaking.
interface AdHocRequest {
  queries?: PinotDataQuery[];
  timeRange?: { from: DateTime; to: DateTime };
}

function readAdHocRequest(options?: AdHocRequest): AdHocRequest {
  return options ?? {};
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
