import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { QueryType } from '../dataquery/QueryType';
import { VariableType } from '../components/VariableQueryEditor/SelectVariableType';
import { ColumnTypes } from '../components/VariableQueryEditor/SelectColumnType';

export interface VariableParams {
  tableName: string;
  variableType: string;
  columnName: string;
  columnType: string;
  pinotQlCode: string;
}

export function variableParamsFrom(query: PinotDataQuery): VariableParams {
  return {
    variableType: query.variableQuery?.variableType || '',
    tableName: query.tableName || '',
    columnName: query.variableQuery?.columnName || '',
    columnType: query.variableQuery?.columnType || '',
    pinotQlCode: query.variableQuery?.pinotQlCode || '',
  };
}

export function applyDefaults(params: VariableParams): boolean {
  let changed = false;
  if (params.variableType === '') {
    changed = true;
    params.variableType = VariableType.TableList;
  }

  if (params.variableType === VariableType.ColumnList && params.columnType === '') {
    changed = true;
    params.columnType = ColumnTypes.All;
  }
  return changed;
}

export function dataQueryWithVariableParams(query: PinotDataQuery, params: VariableParams): PinotDataQuery {
  return {
    ...query,
    queryType: QueryType.PinotVariableQuery,
    tableName: params.tableName || undefined,
    variableQuery: {
      variableType: params.variableType || VariableType.TableList,
      columnName: params.columnName || undefined,
      columnType: params.columnType || ColumnTypes.All,
      pinotQlCode: params.pinotQlCode || undefined,
    },
  };
}
