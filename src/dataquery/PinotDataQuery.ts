import { DataQuery } from '@grafana/schema';
import { DimensionFilter } from './DimensionFilter';
import { OrderByClause } from './OrderByClause';
import { QueryOption } from './QueryOption';
import { getTemplateSrv } from '@grafana/runtime';
import { ScopedVars } from '@grafana/data';
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

export function interpolateVariables(query: PinotDataQuery, scopedVars?: ScopedVars): PinotDataQuery {
  const templateSrv = getTemplateSrv();

  function mapIfExists<T>(target: T | undefined, mapper: (val: T) => T): T | undefined {
    return target ? mapper(target) : undefined;
  }

  const replace = (target: string) => templateSrv.replace(target, scopedVars);
  const replaceIfExists = (target?: string | null) => (target ? replace(target) : undefined);

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
    metadataColumns: query.metadataColumns?.map(({ name, key }) => ({
      name: replaceIfExists(name),
      key: replaceIfExists(key),
    })),
    jsonExtractors: query.jsonExtractors?.map(({ source, path, resultType, alias }) => ({
      source: mapIfExists(source, ({ name, key }) => ({
        name: replaceIfExists(name),
        key: replaceIfExists(key),
      })),
      alias: replaceIfExists(alias),
      path: replaceIfExists(path),
      resultType,
    })),
    regexpExtractors: query.regexpExtractors?.map(({ source, pattern, group, alias }) => ({
      source: mapIfExists(source, ({ name, key }) => ({
        name: replaceIfExists(name),
        key: replaceIfExists(key),
      })),
      alias: replaceIfExists(alias),
      pattern: replaceIfExists(pattern),
      group,
    })),
    filters: query.filters?.map(({ columnName, columnKey, operator, valueExprs }) => ({
      columnName: replaceIfExists(columnName),
      columnKey: replaceIfExists(columnKey),
      operator,
      valueExprs: valueExprs?.map((expr) => replace(expr)),
    })),
    queryOptions: query.queryOptions?.map(({ name, value }) => ({
      name: replaceIfExists(name),
      value: replaceIfExists(value),
    })),

    // Sql Editor

    pinotQlCode: replaceIfExists(query.pinotQlCode),

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
