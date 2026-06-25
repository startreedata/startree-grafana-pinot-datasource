import {
  AdHocVariableFilter,
  CoreApp,
  DataQueryRequest,
  DataQueryResponse,
  DataSourceGetTagKeysOptions,
  DataSourceGetTagValuesOptions,
  DataSourceInstanceSettings,
  DateTime,
  dateTime,
  LogRowContextOptions,
  LogRowContextQueryDirection,
  LogRowModel,
  MetricFindValue,
  ScopedVars,
  SupplementaryQueryOptions,
  SupplementaryQueryType,
  TimeRange,
} from '@grafana/data';
import { DataSourceWithBackend } from '@grafana/runtime';
// Match the rxjs instance the SDK returns (see variables.ts) so Observable types line up across
// the duplicate rxjs installs.
import { lastValueFrom, map, Observable } from '@grafana/data/node_modules/rxjs';

import { interpolateVariables, PinotDataQuery } from './dataquery/PinotDataQuery';
import { PinotConnectionConfig } from './config/PinotConnectionConfig';
import { PinotVariableSupport } from './variables';
import { AnnotationsQueryEditor } from './components/AnnotationsQueryEditor/AnnotationsQueryEditor';
import { listColumns } from './resources/columns';
import { queryDistinctValuesForFilters } from './resources/distinctValues';
import {
  attachDerivedFieldLinks,
  DEFAULT_LOG_ROW_CONTEXT_LIMIT,
  LOG_ROW_CONTEXT_REF_ID,
  logRowContextQuery,
  logRowContextTimeWindow,
  logsVolumeQuery,
} from './logs';

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

  query(request: DataQueryRequest<PinotDataQuery>): Observable<DataQueryResponse> {
    // Surface extractor-derived fields with data links on the returned logs frames. No-op for
    // frames without extractors-with-links (e.g. time series, volume).
    return super.query(request).pipe(map((response) => attachDerivedFieldLinks(response, request.targets)));
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

  // --- Logs volume histogram (Explore supplementary query) ---

  getSupportedSupplementaryQueryTypes(): SupplementaryQueryType[] {
    return [SupplementaryQueryType.LogsVolume];
  }

  getSupplementaryQuery(options: SupplementaryQueryOptions, query: PinotDataQuery): PinotDataQuery | undefined {
    if (options.type !== SupplementaryQueryType.LogsVolume) {
      return undefined;
    }
    return logsVolumeQuery(query);
  }

  getSupplementaryRequest(
    type: SupplementaryQueryType,
    request: DataQueryRequest<PinotDataQuery>
  ): DataQueryRequest<PinotDataQuery> | undefined {
    if (type !== SupplementaryQueryType.LogsVolume) {
      return undefined;
    }
    const targets = request.targets
      .map((query) => this.getSupplementaryQuery({ type }, query))
      .filter((query): query is PinotDataQuery => query !== undefined);
    if (targets.length === 0) {
      return undefined;
    }
    return { ...request, requestId: `${request.requestId}_log_volume`, targets };
  }

  // --- Log row context (Explore "show context") ---

  showContextToggle(): boolean {
    return true;
  }

  async getLogRowContext(
    row: LogRowModel,
    options?: LogRowContextOptions,
    query?: PinotDataQuery
  ): Promise<DataQueryResponse> {
    // Context needs the originating logs query (table/log/time/filters) to fetch neighbours on the
    // same table; without it there's nothing to anchor to.
    if (!query?.tableName || !query?.timeColumn) {
      return { data: [] };
    }

    const direction = options?.direction ?? LogRowContextQueryDirection.Backward;
    const limit = options?.limit ?? DEFAULT_LOG_ROW_CONTEXT_LIMIT;
    const { fromMs, toMs } = logRowContextTimeWindow(row.timeEpochMs, direction);
    const from = dateTime(fromMs);
    const to = dateTime(toMs);
    const range: TimeRange = { from, to, raw: { from, to } };

    const request: DataQueryRequest<PinotDataQuery> = {
      requestId: `${LOG_ROW_CONTEXT_REF_ID}-${direction}-${row.uid}`,
      interval: '1s',
      intervalMs: 1000,
      range,
      scopedVars: {},
      timezone: 'UTC',
      app: CoreApp.Explore,
      startTime: from.valueOf(),
      targets: [logRowContextQuery(query, direction, limit)],
    };

    return lastValueFrom(this.query(request));
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
