import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { QueryType } from '../dataquery/QueryType';

export interface Params {
  tableName: string;
  promQlCode: string;
  legend: string;
}

export function paramsFrom(query: PinotDataQuery): Params {
  return {
    tableName: query.tableName || '',
    promQlCode: query.promQlCode || '',
    legend: query.legend || '',
  };
}

export function dataQueryWithParams(query: PinotDataQuery, params: Params): PinotDataQuery {
  return {
    ...query,
    queryType: QueryType.PromQL,
    tableName: params.tableName || undefined,
    promQlCode: params.promQlCode || undefined,
    legend: params.legend || undefined,
  };
}
