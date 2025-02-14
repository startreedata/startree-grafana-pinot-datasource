import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { QueryType } from '../dataquery/QueryType';

export interface Params {
  tableName: string;
  promQlCode: string;
  legend: string;
  seriesLimit: number;
}

export function paramsFrom(query: PinotDataQuery): Params {
  return {
    tableName: query.tableName || '',
    promQlCode: query.promQlCode || '',
    legend: query.legend || '',
    seriesLimit: query.seriesLimit || 0,
  };
}

export function dataQueryOf(query: PinotDataQuery, params: Params): PinotDataQuery {
  return {
    ...query,
    queryType: QueryType.PromQL,
    tableName: params.tableName || undefined,
    promQlCode: params.promQlCode || undefined,
    legend: params.legend || undefined,
    seriesLimit: params.seriesLimit || undefined,
  };
}
