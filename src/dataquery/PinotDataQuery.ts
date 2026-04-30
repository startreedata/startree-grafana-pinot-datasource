import { DataQuery } from '@grafana/schema';
import { DimensionFilter } from './DimensionFilter';
import { OrderByClause } from './OrderByClause';
import { QueryOption } from './QueryOption';
import { getTemplateSrv } from '@grafana/runtime';
import { ScopedVars, TypedVariableModel, QueryVariableModel } from '@grafana/data';
import { PinotVariableQuery } from './PinotVariableQuery';
import { ComplexField } from './ComplexField';
import { JsonExtractor } from './JsonExtractor';
import { RegexpExtractor } from './RegexpExtractor';

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

function isQueryVariable(v: TypedVariableModel): v is QueryVariableModel {
  return v.type === 'query';
}

function getVariableSubquery(variable: QueryVariableModel): string | undefined {
  const varQuery = variable.query as PinotDataQuery | undefined;
  return varQuery?.variableQuery?.pinotQlCode;
}

function getAllOptions(variable: QueryVariableModel): string[] {
  return (variable.options ?? [])
    .map((opt) => (typeof opt.value === 'string' ? opt.value : ''))
    .filter((v) => v !== '' && v !== '$__all');
}

function getSelectedValues(variable: QueryVariableModel): string[] {
  const current = variable.current;
  if (!current || !('value' in current)) {
    return [];
  }
  const value = current.value;
  if (typeof value === 'string') {
    return value === '$__all' ? getAllOptions(variable) : [value];
  }
  if (Array.isArray(value)) {
    return value.includes('$__all') ? getAllOptions(variable) : value.filter((v) => v !== '$__all');
  }
  return [];
}

function escapeSqlString(value: string): string {
  return value.replace(/'/g, "''");
}

function extractSubqueryColumn(subquery: string): string | undefined {
  const match = subquery.match(/SELECT\s+(?:DISTINCT\s+)?["`]?(\w+)["`]?\s+FROM/i);
  return match?.[1];
}

function computeSubqueryReplacement(
  allOptions: string[],
  selectedValues: string[],
  subquery: string
): string | null {
  const selectedCount = selectedValues.length;
  const totalCount = allOptions.length;

  if (selectedCount <= IN_CLAUSE_THRESHOLD) {
    return null;
  }

  if (selectedCount >= totalCount) {
    return subquery;
  }

  const selectedSet = new Set(selectedValues);
  const excludedValues = allOptions.filter((v) => !selectedSet.has(v));

  if (excludedValues.length > IN_CLAUSE_THRESHOLD) {
    return subquery;
  }

  const column = extractSubqueryColumn(subquery);
  if (!column) {
    return subquery;
  }

  const excludedLiterals = excludedValues.map((v) => `'${escapeSqlString(v)}'`).join(', ');
  const notInClause = `"${column}" NOT IN (${excludedLiterals})`;
  const hasWhere = /\bWHERE\b/i.test(subquery);

  if (hasWhere) {
    return `${subquery} AND ${notInClause}`;
  } else {
    return `${subquery} WHERE ${notInClause}`;
  }
}

export function replaceVariablesWithSubquery(sql: string, variables: TypedVariableModel[]): string {
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

    const replacement = computeSubqueryReplacement(allOptions, selectedValues, subquery);
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

function computeBuilderFilterSubquery(
  valueExprs: string[] | undefined,
  variables: TypedVariableModel[],
  operator: string | undefined
): { operator: string; subqueryExpr: string } | null {
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
    if (selectedValues.length <= IN_CLAUSE_THRESHOLD) {
      continue;
    }

    const subqueryExpr = computeSubqueryReplacement(allOptions, selectedValues, subquery);
    if (!subqueryExpr) {
      continue;
    }

    const newOperator = operator === '!=' || operator === 'not in' ? 'not in' : 'in';
    return { operator: newOperator, subqueryExpr };
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

  let subqueryUsed = false;

  const filters = query.filters?.map(({ columnName, columnKey, operator, valueExprs }) => {
    const subquery = computeBuilderFilterSubquery(valueExprs, variables, operator);
    if (subquery) {
      subqueryUsed = true;
      return {
        columnName: replaceIfExists(columnName),
        columnKey: replaceIfExists(columnKey),
        operator: subquery.operator,
        subqueryExpr: subquery.subqueryExpr,
      };
    }
    return {
      columnName: replaceIfExists(columnName),
      columnKey: replaceIfExists(columnKey),
      operator,
      valueExprs: valueExprs?.map((expr) => replace(expr)),
    };
  });

  let pinotQlCode: string | undefined;
  if (query.pinotQlCode) {
    const afterSubquery = replaceVariablesWithSubquery(query.pinotQlCode, variables);
    if (afterSubquery !== query.pinotQlCode) {
      subqueryUsed = true;
    }
    pinotQlCode = replaceIfExists(afterSubquery);
  } else {
    pinotQlCode = replaceIfExists(query.pinotQlCode);
  }

  let queryOptions = query.queryOptions?.map(({ name, value }) => ({
    name: replaceIfExists(name),
    value: replaceIfExists(value),
  }));

  if (subqueryUsed) {
    const hasMultistage = queryOptions?.some(
      (o) => o.name?.toLowerCase() === 'usemultistageengine'
    );
    if (!hasMultistage) {
      queryOptions = [...(queryOptions ?? []), { name: 'useMultiStageEngine', value: 'true' }];
    }
  }

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
