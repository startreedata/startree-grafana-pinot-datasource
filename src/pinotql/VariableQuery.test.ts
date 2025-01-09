import { VariableType } from '../components/VariableQueryEditor/SelectVariableType';
import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { QueryType } from '../dataquery/QueryType';
import { ColumnTypes } from '../components/VariableQueryEditor/SelectColumnType';
import { applyDefaults, dataQueryOf, Params, paramsFrom } from './VariableQuery';

describe('paramsFrom', () => {
  test('query is empty', () => {
    expect(paramsFrom({ refId: 'test_id' })).toEqual<Params>({
      tableName: '',
      variableType: '',
      columnName: '',
      columnType: '',
      pinotQlCode: '',
    });
  });

  test('query is fully populated', () => {
    expect(
      paramsFrom({
        refId: 'test_id',
        tableName: 'test_table',
        variableQuery: {
          variableType: VariableType.PinotQlCode,
          columnName: 'test_column_name',
          columnType: ColumnTypes.Metric,
          pinotQlCode: 'SELECT * FROM "test_table";',
        },
      })
    ).toEqual<Params>({
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
    const params: Params = {
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
    const params: Params = {
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
    const params: Params = {
      tableName: 'test_table',
      variableType: VariableType.PinotQlCode,
      columnName: 'test_column_name',
      columnType: ColumnTypes.Metric,
      pinotQlCode: 'SELECT * FROM "test_table";',
    };
    expect(applyDefaults(params)).toEqual(false);
  });
});

describe('dataQueryOf', () => {
  const query: PinotDataQuery = { refId: 'test_id' };
  test('params are empty', () => {
    expect(
      dataQueryOf(query, {
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
      dataQueryOf(query, {
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
