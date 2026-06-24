import { DataQuery } from '@grafana/schema';
import { DimensionFilter } from './DimensionFilter';
import { OrderByClause } from './OrderByClause';
import { QueryOption } from './QueryOption';
import { getTemplateSrv } from '@grafana/runtime';
import { ScopedVars, TypedVariableModel } from '@grafana/data';
import { PinotVariableQuery } from './PinotVariableQuery';
import { ComplexField } from './ComplexField';
import { JsonExtractor } from './JsonExtractor';
import { RegexpExtractor } from './RegexpExtractor';
import { buildFilterSubqueryReplacement, escapeSqlString } from '../utils/subquery.util';
import { isQueryVariable, getVariableSubquery, getAllOptions, getSelectedValues } from './variableQuery.util';

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
  logColumn?: ComplexField;
  metadataColumns?: ComplexField[];
  jsonExtractors?: JsonExtractor[];
  regexpExtractors?: RegexpExtractor[];
  seriesLimit?: number;

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

export function interpolateVariables(query: PinotDataQuery, scopedVars?: ScopedVars): PinotDataQuery {
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
    let pinotQlCodeAfterSubqueryReplacement = query.pinotQlCode
      ? replaceAllVariableExpressionsWithSubqueries(query.pinotQlCode, variables)
      : undefined;

    if (pinotQlCodeAfterSubqueryReplacement !== undefined &&
        pinotQlCodeAfterSubqueryReplacement !== query.pinotQlCode) {
      // Subquery replaced a variable — MSE is required for subquery execution.
      // Append SET so it's visible in SQL Preview and sent to Pinot.
      pinotQlCodeAfterSubqueryReplacement = appendSetMultiStageEngine(pinotQlCodeAfterSubqueryReplacement);
    } else if (pinotQlCodeAfterSubqueryReplacement === undefined) {
      pinotQlCodeAfterSubqueryReplacement = query.pinotQlCode;
    }

    return { pinotQlCode: replaceIfExists(pinotQlCodeAfterSubqueryReplacement) };
  }

  const { filters, queryOptions } = interpolateVariablesInBuilderMode();
  const { pinotQlCode } = interpolateVariablesInCodeMode();

  return {
    ...query,

    // Table name (supports a dashboard variable like `$table`); must be resolved before
    // `$__table()` expansion and the schema/config lookups on the backend.
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
    granularity: replaceIfExists(query.granularity),
    aggregationFunction: replaceIfExists(query.aggregationFunction),
    groupByColumns: query.groupByColumns?.map((columnName) => replace(columnName)),
    groupByColumnsV2: query.groupByColumnsV2?.map(({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
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
    jsonExtractors: query.jsonExtractors?.map(({ source, path, resultType, alias }) => ({
      source: mapIfExists(source, ({ name, key }) => ({
        name: replaceIfExists(name),
        key: replaceIfExists(key),
      })),
      path,
      resultType,
      alias: replaceIfExists(alias),
    })),
    regexpExtractors: query.regexpExtractors?.map(({ source, pattern, group, alias }) => ({
      source: mapIfExists(source, ({ name, key }) => ({
        name: replaceIfExists(name),
        key: replaceIfExists(key),
      })),
      pattern,
      alias: replaceIfExists(alias),
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
  };
}
