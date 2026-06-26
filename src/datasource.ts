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
import { DimensionFilter } from './dataquery/DimensionFilter';
import { columnLabelOf } from './pinotql/complexField';
import { QueryType } from './dataquery/QueryType';
import { EditorMode } from './dataquery/EditorMode';
import { PinotConnectionConfig } from './config/PinotConnectionConfig';
import { PinotVariableSupport } from './variables';
import { AnnotationsQueryEditor } from './components/AnnotationsQueryEditor/AnnotationsQueryEditor';
import { listColumns } from './resources/columns';
import { queryDistinctValuesForFilters } from './resources/distinctValues';
import {
  attachDerivedFieldLinks,
  attachLogsToTracesLinks,
  DEFAULT_LOG_ROW_CONTEXT_LIMIT,
  LOG_ROW_CONTEXT_REF_ID,
  logRowContextQuery,
  logRowContextTimeWindow,
  logsToTracesConfig,
  LogsToTracesConfig,
  logsVolumeQuery,
} from './logs';

export class DataSource extends DataSourceWithBackend<PinotDataQuery, PinotConnectionConfig> {
  // Table context for ad-hoc filters. Pinot columns are per-table but Grafana's getTagValues
  // gives us only the column key, so we remember the table/time column resolved from the panel
  // queries in getTagKeys and reuse it for getTagValues.
  // ponytail: single cached context, good for single-table dashboards; revisit if a dashboard
  // mixes tables and needs per-key tables.
  private adHocContext?: { tableName: string; timeColumn?: string };

  // Datasource-level logs-to-trace mapping, read once from settings. DataSourceApi doesn't retain
  // instanceSettings, so capture the bits the logs path needs (the mapping + uid/name for the
  // internal data link) here.
  private readonly logsToTraces: LogsToTracesConfig;

  constructor(instanceSettings: DataSourceInstanceSettings<PinotConnectionConfig>) {
    super(instanceSettings);

    this.logsToTraces = logsToTracesConfig(instanceSettings.jsonData);
    this.variables = new PinotVariableSupport(this);
    this.annotations = { QueryEditor: AnnotationsQueryEditor };
  }

  query(request: DataQueryRequest<PinotDataQuery>): Observable<DataQueryResponse> {
    return super.query(request).pipe(
      // Surface extractor-derived fields with data links on the returned logs frames. No-op for
      // frames without extractors-with-links (e.g. time series, volume).
      map((response) => attachDerivedFieldLinks(response, request.targets)),
      // Add the logs-to-trace data link on each log row's trace id. No-op unless the datasource has
      // the mapping configured.
      map((response) => attachLogsToTracesLinks(response, this.logsToTraces, { uid: this.uid, name: this.name }))
    );
  }

  // Summary shown for a query when a panel/query row is collapsed. Without this, Grafana falls back
  // to the raw refId, which tells the user nothing about what the query does.
  getQueryDisplayText(query: PinotDataQuery): string {
    switch (query.queryType) {
      case QueryType.PromQL:
        return query.promQlCode || EMPTY_QUERY_DISPLAY_TEXT;
      case QueryType.PinotVariableQuery:
        return query.variableQuery?.pinotQlCode || query.variableQuery?.columnName || EMPTY_QUERY_DISPLAY_TEXT;
      case QueryType.PinotQL:
        switch (query.editorMode) {
          case EditorMode.Code:
            return query.pinotQlCode || EMPTY_QUERY_DISPLAY_TEXT;
          case EditorMode.Builder: {
            const filters = query.filters?.map(displayFilter).join(', ') || 'none';
            return (
              `Table: ${query.tableName || 'none'}, Time: ${query.timeColumn || 'none'}, ` +
              `Aggregation: ${displayAggregations(query) || 'none'}, ` +
              `Dimensions: ${displayDimensions(query) || 'none'}, Filters: ${filters}`
            );
          }
          default:
            return EMPTY_QUERY_DISPLAY_TEXT;
        }
      default:
        return EMPTY_QUERY_DISPLAY_TEXT;
    }
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

// Shown by getQueryDisplayText when a query has no meaningful content to summarize.
const EMPTY_QUERY_DISPLAY_TEXT = 'Empty query';

// Renders a Builder-mode filter row for the collapsed-panel summary, e.g. `country = 'US','CA'`, or
// `country IN (subquery)` when a template variable expanded past the IN-clause threshold. Includes
// the complex-field key when present so `meta['region']` doesn't collapse to a bare `meta`.
function displayFilter(filter: DimensionFilter): string {
  const column = columnLabelOf(filter.columnName, filter.columnKey);
  const rhs = filter.subqueryExpr ?? filter.valueExprs?.join(',') ?? '';
  return `${column} ${filter.operator ?? ''} ${rhs}`.trim();
}

// Summarizes the Builder aggregations for the collapsed panel. Handles both builder shapes: the
// table builder's `aggregations[]` (e.g. `SUM(value), COUNT(*)`) and the time-series builder's
// single `aggregationFunction` + metric column (newer `metricColumnV2`, falling back to the legacy
// `metricColumn`).
function displayAggregations(query: PinotDataQuery): string {
  if (query.aggregations?.length) {
    return query.aggregations
      .map((agg) => `${agg.function ?? ''}(${columnLabelOf(agg.column?.name, agg.column?.key) || '*'})`)
      .join(', ');
  }
  if (query.aggregationFunction) {
    const metric = columnLabelOf(query.metricColumnV2?.name, query.metricColumnV2?.key) || query.metricColumn;
    return metric ? `${query.aggregationFunction}(${metric})` : query.aggregationFunction;
  }
  return '';
}

// Merges legacy string dimensions (`groupByColumns`) with the newer complex-field dimensions
// (`groupByColumnsV2`) that current builders write.
function displayDimensions(query: PinotDataQuery): string {
  return [
    ...(query.groupByColumns ?? []),
    ...(query.groupByColumnsV2 ?? []).map((col) => columnLabelOf(col.name, col.key)),
  ]
    .filter(Boolean)
    .join(', ');
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
