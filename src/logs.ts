import { DataFrame, DataQueryResponse, Field, FieldType, LogRowContextQueryDirection } from '@grafana/data';
import { DisplayType } from './dataquery/DisplayType';
import { EditorMode } from './dataquery/EditorMode';
import { QueryType } from './dataquery/QueryType';
import { PinotDataQuery } from './dataquery/PinotDataQuery';

// Prefix for refIds of derived logs queries so they don't collide with the user's panel query.
export const LOGS_VOLUME_REF_ID_PREFIX = 'log-volume-';

export const LOG_ROW_CONTEXT_REF_ID = 'log-row-context';

// ponytail: fixed 1h window each side of the anchor row; the row LIMIT bounds the result, the
// window just keeps Pinot from scanning the whole table. Widen if very sparse logs need it.
export const LOG_ROW_CONTEXT_WINDOW_MS = 60 * 60 * 1000;

export const DEFAULT_LOG_ROW_CONTEXT_LIMIT = 10;

// A logs builder query is runnable (and derivable) once it has a table and a time column.
export function isLogsBuilderQuery(query: PinotDataQuery): boolean {
  return (
    !query.hide &&
    query.queryType === QueryType.PinotQL &&
    query.editorMode === EditorMode.Builder &&
    query.displayType === DisplayType.LOGS &&
    !!query.tableName &&
    !!query.timeColumn
  );
}

// logsVolumeQuery derives the count(*)-over-time supplementary query from a logs builder query.
// The `logsVolume` flag routes the backend to LogsBuilderQuery.VolumeQuery(); the level column
// (if any) carries over so the volume is broken down by level. Returns undefined when the query
// isn't a runnable logs builder query.
export function logsVolumeQuery(query: PinotDataQuery): PinotDataQuery | undefined {
  if (!isLogsBuilderQuery(query)) {
    return undefined;
  }
  return {
    ...query,
    refId: `${LOGS_VOLUME_REF_ID_PREFIX}${query.refId}`,
    logsVolume: true,
  };
}

// logRowContextQuery derives the query that fetches the rows around an anchor log row: the same
// table/log/time/filters as the panel query, but with the context direction and row limit.
export function logRowContextQuery(
  query: PinotDataQuery,
  direction: LogRowContextQueryDirection,
  limit: number
): PinotDataQuery {
  return {
    ...query,
    refId: LOG_ROW_CONTEXT_REF_ID,
    logContextDirection: direction,
    logsVolume: undefined,
    limit,
  };
}

// logRowContextTimeWindow returns the [from, to] epoch-ms window to query for context rows. BACKWARD
// looks before the anchor, FORWARD after; combined with the backend's direction-aware sort + LIMIT
// this yields the rows nearest the anchor on the requested side.
export function logRowContextTimeWindow(
  anchorMs: number,
  direction: LogRowContextQueryDirection
): { fromMs: number; toMs: number } {
  if (direction === LogRowContextQueryDirection.Forward) {
    return { fromMs: anchorMs, toMs: anchorMs + LOG_ROW_CONTEXT_WINDOW_MS };
  }
  return { fromMs: anchorMs - LOG_ROW_CONTEXT_WINDOW_MS, toMs: anchorMs };
}

interface LinkedExtractor {
  alias: string;
  link: string;
}

function linkedExtractors(query: PinotDataQuery): LinkedExtractor[] {
  return [...(query.jsonExtractors ?? []), ...(query.regexpExtractors ?? [])]
    .filter((e): e is { alias: string; link: string } => !!e.alias && !!e.link)
    .map(({ alias, link }) => ({ alias, link }));
}

// attachDerivedFieldLinks surfaces extractor-derived values (already computed by the backend and
// folded into each logs frame's `labels` field) as standalone fields carrying Grafana data links,
// so they render as clickable links in the expanded log row. The extraction itself stays on the
// backend — this only reads the values back out and attaches the link config.
export function attachDerivedFieldLinks(response: DataQueryResponse, targets: PinotDataQuery[]): DataQueryResponse {
  const data = (response.data ?? []).map((frame: DataFrame) => {
    const target = targets.find((t) => t.refId === frame.refId) ?? (targets.length === 1 ? targets[0] : undefined);
    const extractors = target ? linkedExtractors(target) : [];
    const labelsField = frame.fields?.find((f) => f.name === 'labels');
    if (extractors.length === 0 || !labelsField) {
      return frame;
    }

    const labels = labelsField.values as readonly unknown[];
    const derivedFields: Field[] = extractors.map(({ alias, link }) => ({
      name: alias,
      type: FieldType.string,
      config: { links: [{ title: alias, url: link, targetBlank: true }] },
      values: labels.map((l) => labelValue(l, alias)),
    }));

    return { ...frame, fields: [...frame.fields, ...derivedFields] };
  });
  return { ...response, data };
}

// The backend folds extracted columns into the `labels` field; per row this is either a parsed
// object or its JSON string. Pull the value for `alias` back out as a string.
function labelValue(labels: unknown, alias: string): string {
  const obj = typeof labels === 'string' ? safeParseObject(labels) : labels;
  if (obj && typeof obj === 'object') {
    const value = (obj as Record<string, unknown>)[alias];
    return value == null ? '' : String(value);
  }
  return '';
}

function safeParseObject(s: string): unknown {
  try {
    return JSON.parse(s);
  } catch {
    return undefined;
  }
}
