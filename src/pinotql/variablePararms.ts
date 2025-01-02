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
    tableName: query.tableName || '',
    variableType: query.variableQuery?.variableType || VariableType.TableList,
    columnName: query.variableQuery?.columnName || '',
    columnType: query.variableQuery?.columnType || ColumnTypes.All,
    pinotQlCode: query.variableQuery?.pinotQlCode || '',
  };
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
