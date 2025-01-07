import { applyDefaults, dataQueryWithVariableParams, VariableParams, variableParamsFrom } from './variablePararms';
import { VariableType } from '../components/VariableQueryEditor/SelectVariableType';
import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { QueryType } from '../dataquery/QueryType';
import { ColumnTypes } from '../components/VariableQueryEditor/SelectColumnType';

describe('variableParamsFrom', () => {
  test('query is empty', () => {
    expect(variableParamsFrom({ refId: 'test_id' })).toEqual<VariableParams>({
      tableName: '',
      variableType: VariableType.TableList,
      columnName: '',
      columnType: ColumnTypes.All,
      pinotQlCode: '',
    });
  });

  test('query is fully populated', () => {
    expect(
      variableParamsFrom({
        refId: 'test_id',
        tableName: 'test_table',
        variableQuery: {
          variableType: VariableType.PinotQlCode,
          columnName: 'test_column_name',
          columnType: ColumnTypes.Metric,
          pinotQlCode: 'SELECT * FROM "test_table";',
        },
      })
    ).toEqual<VariableParams>({
      tableName: 'test_table',
      variableType: VariableType.PinotQlCode,
      columnName: 'test_column_name',
      columnType: ColumnTypes.Metric,
      pinotQlCode: 'SELECT * FROM "test_table";',
    });
  });
});

describe('applyDefaults', () => {
  test('params are empty', () => {
    const params: VariableParams = {
      tableName: '',
      variableType: '',
      columnName: '',
      columnType: '',
      pinotQlCode: '',
    };
    expect(applyDefaults(params)).toEqual(true);
    expect(params.variableType).toEqual('TABLE_LIST');
  });

  test('no column type', () => {
    const params: VariableParams = {
      tableName: '',
      variableType: 'COLUMN_LIST',
      columnName: '',
      columnType: '',
      pinotQlCode: '',
    };
    expect(applyDefaults(params)).toEqual(true);
    expect(params.columnType).toEqual('ALL');
  });

  test('params are fully populated', () => {
    const params: VariableParams = {
      tableName: 'test_table',
      variableType: VariableType.PinotQlCode,
      columnName: 'test_column_name',
      columnType: ColumnTypes.Metric,
      pinotQlCode: 'SELECT * FROM "test_table";',
    };
    expect(applyDefaults(params)).toEqual(false);
  });
});

describe('dataQueryWithVariableParams', () => {
  const query: PinotDataQuery = { refId: 'test_id' };
  test('params are empty', () => {
    expect(
      dataQueryWithVariableParams(query, {
        tableName: '',
        variableType: '',
        columnName: '',
        columnType: '',
        pinotQlCode: '',
      })
    ).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: QueryType.PinotVariableQuery,
      tableName: undefined,
      variableQuery: {
        variableType: VariableType.TableList,
        columnName: undefined,
        columnType: ColumnTypes.All,
        pinotQlCode: undefined,
      },
    });
  });

  test('params are fully populated', () => {
    expect(
      dataQueryWithVariableParams(query, {
        tableName: 'test_table',
        variableType: VariableType.PinotQlCode,
        columnName: 'test_column_name',
        columnType: ColumnTypes.Metric,
        pinotQlCode: 'SELECT * FROM "test_table";',
      })
    ).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: QueryType.PinotVariableQuery,
      tableName: 'test_table',
      variableQuery: {
        variableType: VariableType.PinotQlCode,
        columnName: 'test_column_name',
        columnType: ColumnTypes.Metric,
        pinotQlCode: 'SELECT * FROM "test_table";',
      },
    });
  });
});
