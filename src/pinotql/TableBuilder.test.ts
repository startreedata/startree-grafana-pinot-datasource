import * as TableBuilder from './TableBuilder';
import { Column } from '../resources/columns';
import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { QueryType } from '../dataquery/QueryType';
import { EditorMode } from '../dataquery/EditorMode';
import { UseResourceResult } from '../resources/UseResourceResult';
import { DisplayType } from '../dataquery/DisplayType';

const newEmptyParams = (): TableBuilder.Params => ({
  tableName: '',
  timeColumn: '',
  dimensions: [],
  aggregations: [],
  limit: 0,
  filters: [],
  orderBy: [],
  queryOptions: [],
});

describe('paramsFrom', () => {
  const query: PinotDataQuery = {
    refId: 'test_id',
    displayType: 'TABLE',
    tableName: 'test_table',
    timeColumn: 'test_time_column',
    groupByColumns: ['dim1'],
    groupByColumnsV2: [{ name: 'dim2', key: 'k' }],
    aggregations: [{ function: 'SUM', column: { name: 'value' } }],
    limit: 100,
    filters: [{ columnName: 'c', operator: '=', valueExprs: ['v'] }],
    orderBy: [{ columnName: 'SUM(value)', direction: 'desc' }],
    queryOptions: [{ name: 'o', value: 'v' }],
  };

  test('query is fully populated', () => {
    expect(TableBuilder.paramsFrom(query)).toEqual<TableBuilder.Params>({
      tableName: 'test_table',
      timeColumn: 'test_time_column',
      dimensions: [{ name: 'dim1' }, { name: 'dim2', key: 'k' }],
      aggregations: [{ function: 'SUM', column: { name: 'value' } }],
      limit: 100,
      filters: [{ columnName: 'c', operator: '=', valueExprs: ['v'] }],
      orderBy: [{ columnName: 'SUM(value)', direction: 'desc' }],
      queryOptions: [{ name: 'o', value: 'v' }],
    });
  });

  test('query is empty', () => {
    expect(TableBuilder.paramsFrom({ refId: 'test_id' })).toEqual<TableBuilder.Params>(newEmptyParams());
  });
});

describe('aggregationLabelOf', () => {
  test('count without a column', () => {
    expect(TableBuilder.aggregationLabelOf({ function: 'COUNT', column: {} })).toEqual('COUNT(*)');
  });
  test('sum of a column', () => {
    expect(TableBuilder.aggregationLabelOf({ function: 'SUM', column: { name: 'value' } })).toEqual('SUM(value)');
  });
  test('max of a complex field', () => {
    expect(TableBuilder.aggregationLabelOf({ function: 'MAX', column: { name: 'attrs', key: 'latency' } })).toEqual(
      "MAX(attrs['latency'])"
    );
  });
  test('count with a literal star column stays COUNT(*)', () => {
    expect(TableBuilder.aggregationLabelOf({ function: 'COUNT', column: { name: '*' } })).toEqual('COUNT(*)');
  });
});

describe('canRunQuery', () => {
  const params: TableBuilder.Params = {
    tableName: 'test_table',
    timeColumn: 'test_time_column',
    dimensions: [{ name: 'dim' }],
    aggregations: [{ function: 'SUM', column: { name: 'value' } }],
    limit: 100,
    filters: [],
    orderBy: [],
    queryOptions: [],
  };

  test('params are empty', () => {
    expect(TableBuilder.canRunQuery(newEmptyParams())).toEqual(false);
  });
  test('tableName is empty', () => {
    expect(TableBuilder.canRunQuery({ ...params, tableName: '' })).toEqual(false);
  });
  test('timeColumn is empty', () => {
    expect(TableBuilder.canRunQuery({ ...params, timeColumn: '' })).toEqual(false);
  });
  test('no dimensions and no valid aggregations', () => {
    expect(
      TableBuilder.canRunQuery({ ...params, dimensions: [], aggregations: [{ function: 'SUM', column: {} }] })
    ).toEqual(false);
  });
  test('dimensions only is runnable', () => {
    expect(TableBuilder.canRunQuery({ ...params, aggregations: [] })).toEqual(true);
  });
  test('count aggregation without a column is runnable', () => {
    expect(
      TableBuilder.canRunQuery({ ...params, dimensions: [], aggregations: [{ function: 'COUNT', column: {} }] })
    ).toEqual(true);
  });
  test('params are fully populated', () => {
    expect(TableBuilder.canRunQuery(params)).toEqual(true);
  });
});

describe('applyDefaults', () => {
  const timeColumns: Column[] = [
    { name: 'ts', dataType: 'TIMESTAMP', key: null, isTime: true, isDerived: false, isMetric: false },
  ];

  test('emptyParams gets a time column and a default COUNT aggregation', () => {
    const params = newEmptyParams();
    expect(TableBuilder.applyDefaults(params, { timeColumns })).toEqual(true);
    expect(params).toEqual<TableBuilder.Params>({
      ...newEmptyParams(),
      timeColumn: 'ts',
      aggregations: [{ function: 'COUNT', column: {} }],
    });
  });

  test('populatedParams is unchanged', () => {
    const params: TableBuilder.Params = {
      tableName: 'test_table',
      timeColumn: 'test_time_column',
      dimensions: [{ name: 'dim' }],
      aggregations: [{ function: 'SUM', column: { name: 'value' } }],
      limit: 100,
      filters: [],
      orderBy: [],
      queryOptions: [],
    };
    expect(TableBuilder.applyDefaults(params, { timeColumns })).toEqual(false);
  });
});

describe('dataQueryOf', () => {
  const query = { refId: 'test_id' };

  test('params are empty', () => {
    expect(TableBuilder.dataQueryOf(query, newEmptyParams())).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: QueryType.PinotQL,
      editorMode: EditorMode.Builder,
      displayType: DisplayType.TABLE,
      tableName: undefined,
      timeColumn: undefined,
      groupByColumns: undefined,
      groupByColumnsV2: undefined,
      aggregations: undefined,
      filters: undefined,
      orderBy: undefined,
      queryOptions: undefined,
      limit: undefined,
    });
  });

  test('params are fully populated', () => {
    expect(
      TableBuilder.dataQueryOf(query, {
        tableName: 'test_table',
        timeColumn: 'test_time_column',
        dimensions: [{ name: 'dim' }],
        aggregations: [{ function: 'SUM', column: { name: 'value' } }],
        limit: 100,
        filters: [{ columnName: 'c', operator: '=', valueExprs: ['v'] }],
        orderBy: [{ columnName: 'SUM(value)', direction: 'desc' }],
        queryOptions: [{ name: 'o', value: 'v' }],
      })
    ).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: QueryType.PinotQL,
      editorMode: EditorMode.Builder,
      displayType: DisplayType.TABLE,
      tableName: 'test_table',
      timeColumn: 'test_time_column',
      groupByColumns: undefined,
      groupByColumnsV2: [{ name: 'dim' }],
      aggregations: [{ function: 'SUM', column: { name: 'value' } }],
      filters: [{ columnName: 'c', operator: '=', valueExprs: ['v'] }],
      orderBy: [{ columnName: 'SUM(value)', direction: 'desc' }],
      queryOptions: [{ name: 'o', value: 'v' }],
      limit: 100,
    });
  });
});

test('resourcesFrom', () => {
  const tablesResult: UseResourceResult<string[]> = { loading: false, result: ['table_1'] };
  const columnsResult: UseResourceResult<Column[]> = {
    loading: false,
    result: [
      { name: 'ts', dataType: 'TIMESTAMP', key: null, isTime: true, isDerived: false, isMetric: false },
      { name: 'ts2', dataType: 'TIMESTAMP', key: null, isTime: true, isDerived: true, isMetric: false },
      { name: 'met', dataType: 'DOUBLE', key: null, isTime: false, isDerived: false, isMetric: true },
      { name: 'dim', dataType: 'STRING', key: null, isTime: false, isDerived: false, isMetric: false },
    ],
  };
  const sqlPreviewResult: UseResourceResult<string> = { loading: false, result: 'SELECT * FROM "table_1";' };

  const got = TableBuilder.resourcesFrom(tablesResult, columnsResult, sqlPreviewResult);
  expect(got.timeColumns).toEqual([
    { name: 'ts', dataType: 'TIMESTAMP', key: null, isTime: true, isDerived: false, isMetric: false },
  ]);
  expect(got.dimensionColumns).toEqual([
    { name: 'met', dataType: 'DOUBLE', key: null, isTime: false, isDerived: false, isMetric: true },
    { name: 'dim', dataType: 'STRING', key: null, isTime: false, isDerived: false, isMetric: false },
  ]);
  expect(got.aggregationColumns).toEqual([
    { name: 'met', dataType: 'DOUBLE', key: null, isTime: false, isDerived: false, isMetric: true },
  ]);
  expect(got.filterColumns).toEqual(got.dimensionColumns);
  expect(got.sqlPreview).toEqual('SELECT * FROM "table_1";');
});
