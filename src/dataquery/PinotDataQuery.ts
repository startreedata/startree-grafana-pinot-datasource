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
    const patterns = [
      new RegExp(`\\$\\{${name}:singlequote\\}`, 'g'),
      new RegExp(`\\$\\{${name}:csv\\}`, 'g'),
      new RegExp(`\\$\\{${name}:pipe\\}`, 'g'),
      new RegExp(`\\$\\{${name}\\}`, 'g'),
      new RegExp(`\\$${name}(?![\\w])`, 'g'),
    ];

    for (const pattern of patterns) {
      result = result.replace(pattern, replacement);
    }
  }

  return result;
}

function appendSetMultiStageEngine(sql: string): string {
  // Skip if user has already set useMultiStageEngine — respect their explicit value (true or false).
  if (/SET\s+useMultiStageEngine\s*=/i.test(sql)) {
    return sql;
  }
  const trimmed = sql.trimEnd();
  const separator = trimmed.endsWith(';') ? '\n' : ';\n';
  return `${trimmed}${separator}\nSET useMultiStageEngine=true;`;
}

function computeBuilderFilterSubquery(
  valueExprs: string[] | undefined,
  variables: TypedVariableModel[],
  operator: string | undefined
): { operator: string; subqueryExpr?: string; valueExprs?: string[] } | null {
  if (!valueExprs || valueExprs.length === 0) {
    return null;
  }

  for (const expr of valueExprs) {
    const varMatch = expr.match(/\$\{?(\w+)/);
    if (!varMatch) {
      continue;
    }

    const varName = varMatch[1];
    const variable = variables.find((v) => v.name === varName);
    if (!variable || !isQueryVariable(variable)) {
      continue;
    }

    const subquery = getVariableSubquery(variable);
    if (!subquery) {
      continue;
    }

    const allOptions = getAllOptions(variable);
    const selectedValues = getSelectedValues(variable);

    const filterSubqueryReplacement = buildFilterSubqueryReplacement(allOptions, selectedValues, subquery, IN_CLAUSE_THRESHOLD);
    if (filterSubqueryReplacement) {
      const newOperator = operator === '!=' || operator === 'not in' ? 'not in' : 'in';
      return { operator: newOperator, subqueryExpr: filterSubqueryReplacement };
    }

    // Below threshold — expand as properly quoted SQL literals rather than letting
    // templateSrv.replace() produce Grafana's unquoted or {val1,val2,...} format which is not valid SQL.
    // For multiple values remap operator to in/not in; for a single value keep the original operator.
    if (selectedValues.length > 1) {
      const newOperator = operator === '!=' || operator === 'not in' ? 'not in' : 'in';
      return { operator: newOperator, valueExprs: selectedValues.map((v) => `'${escapeSqlString(v)}'`) };
    }
    if (selectedValues.length === 1) {
      return { operator: operator ?? '=', valueExprs: [`'${escapeSqlString(selectedValues[0])}'`] };
    }
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
      // Check if user has already configured useMultiStageEngine — don't override their explicit setting.
      const hasUseMultiStageEngineConfigured = queryOptions?.some(
        (queryOption) => queryOption.name?.toLowerCase() === 'usemultistageengine'
      );
      if (!hasUseMultiStageEngineConfigured) {
        queryOptions = [...(queryOptions ?? []), { name: 'useMultiStageEngine', value: 'true' }];
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
