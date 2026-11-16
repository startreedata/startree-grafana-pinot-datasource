import { ScopedVars } from '@grafana/data';

// Macros and Grafana variables that put the dashboard time range into a query.
const TIME_RANGE_REFERENCE =
  /\$__(timeFilter|timeFilterMillis|timeFrom|timeTo|timeFromMillis|timeToMillis|fromTime|toTime)(?![A-Za-z0-9_])|\$\{?__(from|to)(?![A-Za-z0-9_])/;

/** Removes SQL comments so a commented-out macro doesn't count as using the time range. */
function stripSqlComments(sql: string): string {
  return sql.replace(/\/\*[\s\S]*?\*\//g, ' ').replace(/--[^\n]*/g, ' ');
}

/**
 * Whether code-mode SQL, as the user wrote it, references the dashboard time range.
 *
 * This must run before template interpolation: Grafana replaces `$__from`/`$__to` with plain numbers,
 * after which the backend can no longer tell them apart from hard-coded timestamps.
 */
export function sqlUsesTimeRange(sql: string): boolean {
  return TIME_RANGE_REFERENCE.test(stripSqlComments(sql));
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
