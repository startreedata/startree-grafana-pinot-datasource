import { PinotDataQuery } from '../types/PinotDataQuery';
import { DisplayTypeTimeSeries } from '../components/QueryEditor/SelectDisplayType';

export interface CodeParams {
  displayType: string;
  tableName: string;
  pinotQlCode: string;
  timeColumnAlias: string;
  metricColumnAlias: string;
  logColumnAlias: string;
  legend: string;
}

export function codeParamsFrom(query: PinotDataQuery): CodeParams {
  return {
    displayType: query.displayType || DisplayTypeTimeSeries,
    tableName: query.tableName || '',
    pinotQlCode: query.pinotQlCode || '',
    timeColumnAlias: query.timeColumnAlias || '',
    metricColumnAlias: query.metricColumnAlias || '',
    logColumnAlias: query.logColumnAlias || '',
    legend: query.legend || '',
  };
}

export function canRunCodeQuery(params: CodeParams): boolean {
  switch (true) {
    case !params.tableName:
    case !params.pinotQlCode:
      return false;
    default:
      return true;
  }
}

export function dataQueryWithCodeParams(query: PinotDataQuery, params: CodeParams): PinotDataQuery {
  return {
    ...query,
    displayType: params.displayType || undefined,
    tableName: params.tableName || undefined,
    pinotQlCode: params.pinotQlCode || undefined,
    timeColumnAlias: params.timeColumnAlias || undefined,
    metricColumnAlias: params.metricColumnAlias || undefined,
    logColumnAlias: params.logColumnAlias || undefined,
    legend: params.legend || undefined,
  };
}
