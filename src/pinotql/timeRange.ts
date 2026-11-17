import { ScopedVars } from '@grafana/data';

const TIME_RANGE_MACRO =
  /\$__(timeFilter|timeFilterMillis|timeFrom|timeTo|timeFromMillis|timeToMillis|fromTime|toTime|panelMillis)(?![A-Za-z0-9_])/;
const GRAFANA_TIME_RANGE_VARIABLE = /\$\{?__(from|to)(?![A-Za-z0-9_])/;
const PANEL_CONTEXT_MACRO = /\$__(timeGroup|timeAlias|metricAlias|granularityMillis|interval_s|adHocFilter)(?![A-Za-z0-9_])/;

// Marks a query that ignores the dashboard time range on purpose, e.g. `-- $__noTimeRange`.
const NO_TIME_RANGE_MARKER = /\$__noTimeRange(?![A-Za-z0-9_])/;

function sqlHasReference(
  sql: string,
  codeHasReference: (code: string) => boolean,
  quotedReference?: RegExp,
  commentReference?: RegExp
): boolean {
  // SQL escapes a quote by doubling it. Scan whole quoted values and comments as single fragments.
  const fragments = /--[^\r\n]*|\/\*[\s\S]*?(?:\*\/|$)|'(?:''|[^'])*(?:'|$)|"(?:""|[^"])*(?:"|$)|`(?:``|[^`])*(?:`|$)/g;
  let cursor = 0;
  for (let match = fragments.exec(sql); match !== null; match = fragments.exec(sql)) {
    const fragment = match[0];
    if (codeHasReference(sql.slice(cursor, match.index))) {
      return true;
    }
    const isComment = fragment.startsWith('--') || fragment.startsWith('/*');
    if (isComment ? commentReference?.test(fragment) : quotedReference?.test(fragment)) {
      return true;
    }
    cursor = fragments.lastIndex;
  }
  return codeHasReference(sql.slice(cursor));
}

/** Whether raw variable SQL would need its own query context to expand its Pinot macros. */
export function sqlHasContextDependentMacros(sql: string): boolean {
  return sqlHasReference(sql, (code) => TIME_RANGE_MACRO.test(code) || PANEL_CONTEXT_MACRO.test(code));
}

/**
 * Whether code-mode SQL, as the user wrote it, references the dashboard time range.
 *
 * This must run before template interpolation: Grafana replaces `$__from`/`$__to` with plain numbers,
 * after which the backend can no longer tell them apart from hard-coded timestamps.
 *
 * A `$__noTimeRange` marker in SQL code or a comment suppresses the warning.
 */
export function sqlUsesTimeRange(sql: string): boolean {
  return sqlHasReference(
    sql,
    (code) => NO_TIME_RANGE_MARKER.test(code) || TIME_RANGE_MACRO.test(code) || GRAFANA_TIME_RANGE_VARIABLE.test(code),
    // Grafana interpolates variables in quoted SQL too. Backend macros only expand in SQL code.
    GRAFANA_TIME_RANGE_VARIABLE,
    NO_TIME_RANGE_MARKER
  );
}

/** Formats milliseconds as a whole-second Prometheus duration (e.g. `120s`). */
function promDuration(ms: number): string {
  return `${Math.max(1, Math.round(ms / 1000))}s`;
}

/**
 * Replaces `$__rate_interval` in PromQL. Grafana only defines it for its own Prometheus datasource,
 * so without this it reaches StarTree verbatim. Uses Grafana's value when present, otherwise
 * 4 × the panel interval (the scrape interval isn't known here; 4× matches Grafana's lower bound).
 */
export function replaceRateInterval(promQl: string, scopedVars?: ScopedVars): string {
  if (!/\$\{?__rate_interval\b/.test(promQl)) {
    return promQl;
  }
  const grafanaValue = scopedVars?.__rate_interval?.value;
  const intervalMs = Number(scopedVars?.__interval_ms?.value);
  const replacement =
    typeof grafanaValue === 'string' && grafanaValue !== ''
      ? grafanaValue
      : Number.isFinite(intervalMs) && intervalMs > 0
        ? promDuration(4 * intervalMs)
        : undefined;
  if (replacement === undefined) {
    return promQl;
  }
  return promQl.replace(/\$\{__rate_interval\}|\$__rate_interval\b/g, replacement);
}
