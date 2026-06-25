import { DataQuery } from '@grafana/schema';
import { DimensionFilter } from './DimensionFilter';
import { OrderByClause } from './OrderByClause';
import { QueryOption } from './QueryOption';
import { getTemplateSrv } from '@grafana/runtime';
import { AdHocVariableFilter, ScopedVars, TypedVariableModel } from '@grafana/data';
import { PinotVariableQuery } from './PinotVariableQuery';
import { ComplexField } from './ComplexField';
import { Aggregation } from './Aggregation';
import { JsonExtractor } from './JsonExtractor';
import { RegexpExtractor } from './RegexpExtractor';
import { buildFilterSubqueryReplacement, escapeSqlString } from '../utils/subquery.util';
import { isQueryVariable, getVariableSubquery, getAllOptions, getSelectedValues, isAllSelected } from './variableQuery.util';

export interface PinotDataQuery extends DataQuery {
  queryType?: string;
  editorMode?: string;
  tableName?: string;

  // PinotQl Builder
  timeColumn?: string;
  granularity?: string;
  metricColumn?: string;
  groupByColumns?: string[];
  aggregationFunction?: string;
  limit?: number;
  filters?: DimensionFilter[];
  orderBy?: OrderByClause[];
  queryOptions?: QueryOption[];
  legend?: string;
  metricColumnV2?: ComplexField;
  groupByColumnsV2?: ComplexField[];
  aggregations?: Aggregation[];
  logColumn?: ComplexField;
  levelColumn?: ComplexField;
  metadataColumns?: ComplexField[];
  jsonExtractors?: JsonExtractor[];
  regexpExtractors?: RegexpExtractor[];
  seriesLimit?: number;
  // Set by the logs-volume supplementary query to route the logs builder to its count(*) VolumeQuery.
  logsVolume?: boolean;
  // Set by getLogRowContext ("BACKWARD"/"FORWARD") to fetch rows around an anchor log row.
  logContextDirection?: string;

  // PinotQl Traces Builder. Start time / time filter reuse timeColumn above.
  traceIdColumn?: ComplexField;
  spanIdColumn?: ComplexField;
  parentSpanIdColumn?: ComplexField;
  serviceNameColumn?: ComplexField;
  spanNameColumn?: ComplexField;
  durationColumn?: ComplexField;
  durationUnit?: string;
  tagsColumn?: ComplexField;
  statusColumn?: ComplexField;
  traceId?: string;

  // PinotQl Code
  pinotQlCode?: string;
  timeColumnAlias?: string;
  timeColumnFormat?: string;
  metricColumnAlias?: string;
  logColumnAlias?: string;
  displayType?: string;

  // Pinot Variable Query
  variableQuery?: PinotVariableQuery;

  // PromQl
  promQlCode?: string;

  // Grafana ad-hoc filters, injected server-side by the $__adHocFilter macro.
  adHocFilters?: AdHocVariableFilter[];
}

export const IN_CLAUSE_THRESHOLD = 1000;

/**
 * Replaces variable references inside `IN (...)` or `NOT IN (...)` filter positions with the
 * variable's backing subquery, but only when the variable's selection exceeds the threshold
 * (otherwise null is returned and the variable is left for `templateSrv.replace()` to handle).
 *
 * Replacement is intentionally restricted to IN/NOT IN contexts because the subquery is only
 * syntactically valid in those positions. Replacing in `WHERE col = ${var}` would produce
 * invalid SQL like `WHERE col = SELECT ...`.
 *
 * Supported variable formats: `${var:singlequote}`, `${var:csv}`, `${var:pipe}`, `${var}`, `$var`.
 */
export function replaceAllVariableExpressionsWithSubqueries(sql: string, variables: TypedVariableModel[]): string {
  const queryVariables = variables.filter(isQueryVariable);
  if (queryVariables.length === 0) {
    return sql;
  }

  let result = sql;
  for (const variable of queryVariables) {
    const subquery = getVariableSubquery(variable);
    if (!subquery) {
      continue;
    }

    const allOptions = getAllOptions(variable);
    const selectedValues = getSelectedValues(variable);
    if (allOptions.length === 0 || selectedValues.length === 0) {
      continue;
    }

    const replacement = buildFilterSubqueryReplacement(allOptions, selectedValues, subquery, IN_CLAUSE_THRESHOLD);
    if (!replacement) {
      continue;
    }

    const name = variable.name;
    // Each pattern captures the leading `IN (` (or `NOT IN (`) plus whitespace as $1, the variable
    // reference itself, and the trailing `)` plus whitespace as $2 — so we substitute the subquery
    // in place of the variable reference while leaving the surrounding IN(...) syntax untouched.
    const wrappedPatterns = [
      new RegExp(`(\\b(?:NOT\\s+)?IN\\s*\\(\\s*)\\$\\{${name}:singlequote\\}(\\s*\\))`, 'gi'),
      new RegExp(`(\\b(?:NOT\\s+)?IN\\s*\\(\\s*)\\$\\{${name}:csv\\}(\\s*\\))`, 'gi'),
      new RegExp(`(\\b(?:NOT\\s+)?IN\\s*\\(\\s*)\\$\\{${name}:pipe\\}(\\s*\\))`, 'gi'),
      new RegExp(`(\\b(?:NOT\\s+)?IN\\s*\\(\\s*)\\$\\{${name}\\}(\\s*\\))`, 'gi'),
      new RegExp(`(\\b(?:NOT\\s+)?IN\\s*\\(\\s*)\\$${name}(?![\\w])(\\s*\\))`, 'gi'),
    ];

    for (const pattern of wrappedPatterns) {
      result = result.replace(pattern, `$1${replacement}$2`);
    }
  }

  return result;
}

const CONDITIONAL_ALL_MACRO = '$__conditionalAll';

/**
 * Expands `$__conditionalAll(<condition>, $var)` macros in raw PinotQL. When the referenced
 * template variable has its "All" option selected (or is unset), the whole macro collapses to
 * `1=1`, effectively dropping the condition; otherwise it expands to `<condition>` (whose own
 * `$var` references are interpolated later by templateSrv.replace).
 *
 * The condition argument is arbitrary SQL and may contain commas and nested parentheses, so the
 * macro boundary is found by scanning for the matching close paren (respecting single-quoted
 * string literals), and the two arguments are split on top-level commas. Anything that isn't a
 * clean two-argument invocation is left untouched.
 */
export function applyConditionalAll(sql: string, variables: TypedVariableModel[]): string {
  let result = sql;
  let searchFrom = 0;
  for (;;) {
    const start = result.indexOf(`${CONDITIONAL_ALL_MACRO}(`, searchFrom);
    if (start === -1) {
      break;
    }
    const open = start + CONDITIONAL_ALL_MACRO.length; // index of '('
    const close = matchingParen(result, open);
    if (close === -1) {
      break; // unbalanced parens — give up rather than mangle the query
    }
    const args = splitTopLevelArgs(result.slice(open + 1, close));
    if (args.length !== 2) {
      searchFrom = close + 1; // malformed invocation — skip it and keep scanning
      continue;
    }
    const condition = args[0].trim();
    const varName = variableNameOf(args[1].trim());
    const variable = varName ? variables.find((v) => v.name === varName) : undefined;
    const replacement = variable && isAllSelected(variable) ? '1=1' : condition;
    result = result.slice(0, start) + replacement + result.slice(close + 1);
    searchFrom = start + replacement.length;
  }
  return result;
}

// Index of the ')' matching the '(' at openIndex, or -1 if unbalanced. Parens inside single-quoted
// string literals are ignored ('' is an escaped quote).
function matchingParen(s: string, openIndex: number): number {
  let depth = 0;
  let inString = false;
  for (let i = openIndex; i < s.length; i++) {
    const ch = s[i];
    if (inString) {
      if (ch === "'") {
        if (s[i + 1] === "'") {
          i++;
        } else {
          inString = false;
        }
      }
      continue;
    }
    if (ch === "'") {
      inString = true;
    } else if (ch === '(') {
      depth++;
    } else if (ch === ')') {
      depth--;
      if (depth === 0) {
        return i;
      }
    }
  }
  return -1;
}

// Splits on commas at paren depth 0, ignoring commas inside nested parens or string literals.
function splitTopLevelArgs(inner: string): string[] {
  const args: string[] = [];
  let depth = 0;
  let inString = false;
  let last = 0;
  for (let i = 0; i < inner.length; i++) {
    const ch = inner[i];
    if (inString) {
      if (ch === "'") {
        if (inner[i + 1] === "'") {
          i++;
        } else {
          inString = false;
        }
      }
      continue;
    }
    if (ch === "'") {
      inString = true;
    } else if (ch === '(') {
      depth++;
    } else if (ch === ')') {
      depth--;
    } else if (ch === ',' && depth === 0) {
      args.push(inner.slice(last, i));
      last = i + 1;
    }
  }
  args.push(inner.slice(last));
  return args;
}

// Extracts the variable name from a `$var` or `${var}` reference; '' if it isn't a plain reference.
function variableNameOf(ref: string): string {
  const match = ref.match(/^\$\{?(\w+)\}?$/);
  return match ? match[1] : '';
}

function appendSetMultiStageEngine(sql: string): string {
  // Skip if user has already set useMultistageEngine — respect their explicit value (true or false).
  // Match is case-insensitive so we honor either useMultiStageEngine or useMultistageEngine spellings.
  if (/SET\s+useMultistageEngine\s*=/i.test(sql)) {
    return sql;
  }
  const trimmed = sql.trimEnd();
  const separator = trimmed.endsWith(';') ? '\n' : ';\n';
  return `${trimmed}${separator}\nSET useMultistageEngine=true;`;
}

/**
 * Computes the SQL filter replacement for a Builder-mode filter row, aggregating ALL entries
 * in `valueExprs` (any mix of plain literals + Pinot query variable references) into a single
 * filter that includes everything the user picked.
 *
 * @param valueExprs The filter row's existing value expressions, e.g. `["'lit'"]`,
 *                   `["${var}"]`, or mixed like `["'lit'", "$varA", "$varB"]`.
 * @param variables  The dashboard's template variables from `getTemplateSrv()`.
 * @param operator   The filter operator from the row (`=`, `!=`, `in`, `not in`, etc.).
 *
 * @returns
 *   - `null` if no Pinot query variable was found and no expansion happened — caller falls
 *     back to the standard `templateSrv.replace()` per-value interpolation.
 *   - `{ operator, subqueryExpr }` when at least one Pinot query variable's selection exceeds
 *     the threshold. All entries are combined via `UNION ALL` — each subquery wrapped as
 *     `SELECT * FROM (subquery)` and each literal wrapped as `SELECT 'literal'` — and the
 *     filter renders as `(col in (UNION_ALL_RESULT))` on the backend. Operator is remapped
 *     to `in` / `not in`.
 *   - `{ operator, valueExprs }` when all variables (if any) are below threshold. The output
 *     `valueExprs` is the union of plain literals and the variables' expanded literals.
 *     Single-value results preserve the original operator (so `=` stays `=`); multi-value
 *     results remap to `in` / `not in`.
 */
function computeBuilderFilterSubquery(
  valueExprs: string[] | undefined,
  variables: TypedVariableModel[],
  operator: string | undefined
): { operator: string; subqueryExpr?: string; valueExprs?: string[] } | null {
  if (!valueExprs || valueExprs.length === 0) {
    return null;
  }

  // Plain literals from the input (or unhandled variables we leave for the fallback path).
  const literalExprs: string[] = [];
  // Quoted literal expansions from below-threshold Pinot query variables.
  const expandedLiterals: string[] = [];
  // Subquery strings from above-threshold Pinot query variables.
  const subqueryParts: string[] = [];
  let anyPinotVariableHandled = false;

  for (const expr of valueExprs) {
    const varMatch = expr.match(/\$\{?(\w+)/);
    if (!varMatch) {
      literalExprs.push(expr);
      continue;
    }
    const variable = variables.find((v) => v.name === varMatch[1]);
    if (!variable || !isQueryVariable(variable)) {
      literalExprs.push(expr);
      continue;
    }
    const subquery = getVariableSubquery(variable);
    if (!subquery) {
      literalExprs.push(expr);
      continue;
    }
    const allOptions = getAllOptions(variable);
    const selectedValues = getSelectedValues(variable);

    const filterSubqueryReplacement = buildFilterSubqueryReplacement(allOptions, selectedValues, subquery, IN_CLAUSE_THRESHOLD);
    if (filterSubqueryReplacement) {
      subqueryParts.push(filterSubqueryReplacement);
      anyPinotVariableHandled = true;
      continue;
    }
    if (selectedValues.length >= 1) {
      // Below threshold — expand the variable's selected values as properly quoted SQL literals.
      expandedLiterals.push(...selectedValues.map((v) => `'${escapeSqlString(v)}'`));
      anyPinotVariableHandled = true;
      continue;
    }
    // Pinot query variable but no current selection — leave for templateSrv.replace fallback.
    literalExprs.push(expr);
  }

  // If nothing required Pinot-specific handling, return null so the caller's existing
  // per-value templateSrv.replace() path runs.
  if (!anyPinotVariableHandled) {
    return null;
  }

  const allLiterals = [...literalExprs, ...expandedLiterals];

  // Subquery path:
  //  - Single subquery and no literals: pass it through as-is (the common single-variable case).
  //  - Multiple subqueries OR mixed with literals: combine via UNION ALL. Each subquery is wrapped
  //    as `SELECT * FROM (sq)` to handle trailing LIMIT/ORDER BY safely; each literal becomes
  //    `SELECT 'lit'`. Result is plugged into `col IN (UNION_ALL)`.
  if (subqueryParts.length > 0) {
    const newOperator = operator === '!=' || operator === 'not in' ? 'not in' : 'in';
    if (subqueryParts.length === 1 && allLiterals.length === 0) {
      return { operator: newOperator, subqueryExpr: subqueryParts[0] };
    }
    const unionParts = [
      ...subqueryParts.map((sq) => `SELECT * FROM (${sq})`),
      ...allLiterals.map((lit) => `SELECT ${lit}`),
    ];
    return { operator: newOperator, subqueryExpr: unionParts.join(' UNION ALL ') };
  }

  // No subquery needed — return the merged literal list. Single value preserves the
  // original operator so a `=` filter doesn't get remapped to `in` unnecessarily.
  if (allLiterals.length === 1) {
    return { operator: operator ?? '=', valueExprs: allLiterals };
  }
  if (allLiterals.length > 1) {
    const newOperator = operator === '!=' || operator === 'not in' ? 'not in' : 'in';
    return { operator: newOperator, valueExprs: allLiterals };
  }

  return null;
}

export function interpolateVariables(
  query: PinotDataQuery,
  scopedVars?: ScopedVars,
  adHocFilters?: AdHocVariableFilter[]
): PinotDataQuery {
  const templateSrv = getTemplateSrv();
  const variables = templateSrv?.getVariables?.() ?? [];

  function mapIfExists<T>(target: T | undefined, mapper: (val: T) => T): T | undefined {
    return target ? mapper(target) : undefined;
  }

  const replace = (target: string) => templateSrv.replace(target, scopedVars);
  const replaceIfExists = (target?: string | null) => (target ? replace(target) : undefined);

  // --- Builder mode: interpolate filters and inject useMultiStageEngine into queryOptions ---
  function interpolateVariablesInBuilderMode() {
    let subqueryWasInjected = false;

    const filters = query.filters?.map(({ columnName, columnKey, operator, valueExprs }) => {
      const filterSubqueryReplacement = computeBuilderFilterSubquery(valueExprs, variables, operator);
      if (filterSubqueryReplacement) {
        if (filterSubqueryReplacement.subqueryExpr) {
          subqueryWasInjected = true;
        }
        return {
          columnName: replaceIfExists(columnName),
          columnKey: replaceIfExists(columnKey),
          operator: filterSubqueryReplacement.operator,
          ...(filterSubqueryReplacement.subqueryExpr
            ? { subqueryExpr: filterSubqueryReplacement.subqueryExpr }
            : { valueExprs: filterSubqueryReplacement.valueExprs }),
        };
      }
      return {
        columnName: replaceIfExists(columnName),
        columnKey: replaceIfExists(columnKey),
        operator,
        valueExprs: valueExprs?.map((expr) => replace(expr)),
      };
    });

    let queryOptions = query.queryOptions?.map(({ name, value }) => ({
      name: replaceIfExists(name),
      value: replaceIfExists(value),
    }));

    if (subqueryWasInjected) {
      // Check if user has already configured useMultistageEngine — don't override their explicit setting.
      const hasUseMultiStageEngineConfigured = queryOptions?.some(
        (queryOption) => queryOption.name?.toLowerCase() === 'usemultistageengine'
      );
      if (!hasUseMultiStageEngineConfigured) {
        queryOptions = [...(queryOptions ?? []), { name: 'useMultistageEngine', value: 'true' }];
      }
    }

    return { filters, queryOptions };
  }

  // --- Code mode: interpolate pinotQlCode and append SET if subquery was injected ---
  function interpolateVariablesInCodeMode() {
    // Expand $__conditionalAll first so a dropped/kept condition flows through subquery replacement
    // and templateSrv.replace like any other SQL.
    const sqlWithConditionals = query.pinotQlCode
      ? applyConditionalAll(query.pinotQlCode, variables)
      : undefined;

    let pinotQlCodeAfterSubqueryReplacement = sqlWithConditionals
      ? replaceAllVariableExpressionsWithSubqueries(sqlWithConditionals, variables)
      : undefined;

    if (pinotQlCodeAfterSubqueryReplacement !== undefined &&
        pinotQlCodeAfterSubqueryReplacement !== sqlWithConditionals) {
      // Subquery replaced a variable — MSE is required for subquery execution.
      // Append SET so it's visible in SQL Preview and sent to Pinot.
      pinotQlCodeAfterSubqueryReplacement = appendSetMultiStageEngine(pinotQlCodeAfterSubqueryReplacement);
    } else if (pinotQlCodeAfterSubqueryReplacement === undefined) {
      pinotQlCodeAfterSubqueryReplacement = sqlWithConditionals;
    }

    return { pinotQlCode: replaceIfExists(pinotQlCodeAfterSubqueryReplacement) };
  }

  const { filters, queryOptions } = interpolateVariablesInBuilderMode();
  const { pinotQlCode } = interpolateVariablesInCodeMode();

  return {
    ...query,

    // Table name (interpolated so dashboards can bind it to a `$table` template variable; also
    // enables chained COLUMN_LIST variables whose tableName references the selected table).
    tableName: replaceIfExists(query.tableName),

    // Sql Builder

    timeColumn: replaceIfExists(query.timeColumn),
    metricColumn: replaceIfExists(query.metricColumn),
    metricColumnV2: mapIfExists(query.metricColumnV2, ({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),
    logColumn: mapIfExists(query.logColumn, ({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),
    levelColumn: mapIfExists(query.levelColumn, ({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),

    // Traces builder
    traceIdColumn: mapIfExists(query.traceIdColumn, ({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),
    spanIdColumn: mapIfExists(query.spanIdColumn, ({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),
    parentSpanIdColumn: mapIfExists(query.parentSpanIdColumn, ({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),
    serviceNameColumn: mapIfExists(query.serviceNameColumn, ({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),
    spanNameColumn: mapIfExists(query.spanNameColumn, ({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),
    durationColumn: mapIfExists(query.durationColumn, ({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),
    tagsColumn: mapIfExists(query.tagsColumn, ({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),
    statusColumn: mapIfExists(query.statusColumn, ({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),
    traceId: replaceIfExists(query.traceId),

    granularity: replaceIfExists(query.granularity),
    aggregationFunction: replaceIfExists(query.aggregationFunction),
    groupByColumns: query.groupByColumns?.map((columnName) => replace(columnName)),
    groupByColumnsV2: query.groupByColumnsV2?.map(({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),
    aggregations: query.aggregations?.map(({ function: fn, column }) => ({
      function: replaceIfExists(fn),
      column: mapIfExists(column, ({ name, key }) => ({
        name: replaceIfExists(name),
        key: replaceIfExists(key),
      })),
    })),
    orderBy: query.orderBy?.map(({ columnName, columnKey, direction }) => ({
      columnName: replaceIfExists(columnName),
      columnKey: replaceIfExists(columnKey),
      direction,
    })),
    metadataColumns: query.metadataColumns?.map(({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),
    jsonExtractors: query.jsonExtractors?.map(({ source, path, resultType, alias, link }) => ({
      source: mapIfExists(source, ({ name, key }) => ({
        name: replaceIfExists(name),
        key: replaceIfExists(key),
      })),
      path,
      resultType,
      alias: replaceIfExists(alias),
      link: replaceIfExists(link),
    })),
    regexpExtractors: query.regexpExtractors?.map(({ source, pattern, group, alias, link }) => ({
      source: mapIfExists(source, ({ name, key }) => ({
        name: replaceIfExists(name),
        key: replaceIfExists(key),
      })),
      pattern,
      alias: replaceIfExists(alias),
      link: replaceIfExists(link),
      group,
    })),
    filters,
    queryOptions,

    // Sql Editor

    pinotQlCode,

    // PromQl Editor

    promQlCode: replaceIfExists(query.promQlCode),

    // Variable Query editor

    variableQuery: mapIfExists(query.variableQuery, (variableQuery) => ({
      ...variableQuery,
      columnName: replaceIfExists(variableQuery.columnName),
      pinotQlCode: replaceIfExists(variableQuery.pinotQlCode),
    })),

    // Ad-hoc filters: passed straight to the backend for the $__adHocFilter macro to expand.
    adHocFilters,
  };
}
