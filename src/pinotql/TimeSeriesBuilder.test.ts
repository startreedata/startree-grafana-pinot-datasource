import * as TimeSeriesBuilder from './TimeSeriesBuilder';
import { Column } from '../resources/columns';
import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { QueryType } from '../dataquery/QueryType';
import { EditorMode } from '../dataquery/EditorMode';
import { UseResourceResult } from '../resources/UseResourceResult';
import { Granularity } from '../resources/granularities';
import { DisplayType } from '../dataquery/DisplayType';

const newEmptyParams = (): TimeSeriesBuilder.Params => ({
  tableName: '',
  timeColumn: '',
  metricColumn: {},
  granularity: '',
  aggregationFunction: '',
  limit: 0,
  filters: [],
  orderBy: [],
  queryOptions: [],
  legend: '',
  groupByColumns: [],
  seriesLimit: 0,
});

describe('paramsFrom', () => {
  const query: PinotDataQuery = {
    refId: 'test_id',
    displayType: 'TIMESERIES',
    tableName: 'test_table_name',
    timeColumn: 'test_time_column',
    metricColumn: 'test_metric_column1',
    metricColumnV2: { name: 'test_metric_column2', key: 'test_metric_column_key' },
    granularity: 'auto',
    aggregationFunction: 'AVG',
    limit: 100,
    filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
    orderBy: [{ columnName: 'test_order_column', direction: 'asc' }],
    queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
    legend: '{{test_dim_column}}',
    groupByColumns: ['test_dim_column_1'],
    groupByColumnsV2: [{ name: 'test_dim_column2', key: 'test_dim_column2_key' }],
    seriesLimit: 101,
  };

  test('query is fully populated', () => {
    expect(TimeSeriesBuilder.paramsFrom(query)).toEqual<TimeSeriesBuilder.Params>({
      tableName: 'test_table_name',
      timeColumn: 'test_time_column',
      metricColumn: { name: 'test_metric_column2', key: 'test_metric_column_key' },
      granularity: 'auto',
      aggregationFunction: 'AVG',
      limit: 100,
      filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
      orderBy: [{ columnName: 'test_order_column', direction: 'asc' }],
      queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
      legend: '{{test_dim_column}}',
      groupByColumns: [{ name: 'test_dim_column_1' }, { name: 'test_dim_column2', key: 'test_dim_column2_key' }],
      seriesLimit: 101,
    });
  });

  test('metricColumnV2 is absent', () => {
    expect(TimeSeriesBuilder.paramsFrom({ ...query, metricColumnV2: undefined })).toEqual<TimeSeriesBuilder.Params>({
      tableName: 'test_table_name',
      timeColumn: 'test_time_column',
      metricColumn: { name: 'test_metric_column1' },
      granularity: 'auto',
      aggregationFunction: 'AVG',
      limit: 100,
      filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
      orderBy: [{ columnName: 'test_order_column', direction: 'asc' }],
      queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
      legend: '{{test_dim_column}}',
      groupByColumns: [{ name: 'test_dim_column_1' }, { name: 'test_dim_column2', key: 'test_dim_column2_key' }],
      seriesLimit: 101,
    });
  });

  test('query is empty', () => {
    expect(TimeSeriesBuilder.paramsFrom({ refId: 'test_id' })).toEqual<TimeSeriesBuilder.Params>({
      tableName: '',
      timeColumn: '',
      metricColumn: {},
      granularity: '',
      aggregationFunction: '',
      limit: 0,
      filters: [],
      orderBy: [],
      queryOptions: [],
      legend: '',
      groupByColumns: [],
      seriesLimit: 0
    });
  });
});

describe('canRunQuery', () => {
  const params: TimeSeriesBuilder.Params = {
    tableName: 'test_table_name',
    timeColumn: 'test_time_column',
    metricColumn: { name: 'test_metric_column', key: 'test_metric_column_key' },
    granularity: 'auto',
    aggregationFunction: 'AVG',
    limit: 100,
    filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
    orderBy: [{ columnName: 'test_order_column', direction: 'asc' }],
    queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
    legend: '{{test_dim_column}}',
    groupByColumns: [{ name: 'test_dim_column' }],
    seriesLimit: 101,
  };

  test('params are empty', () => {
    expect(TimeSeriesBuilder.canRunQuery(newEmptyParams())).toEqual(false);
  });

  test('tableName is empty', () => {
    expect(TimeSeriesBuilder.canRunQuery({ ...params, tableName: '' })).toEqual(false);
  });

  test('timeColumn is empty', () => {
    expect(TimeSeriesBuilder.canRunQuery({ ...params, timeColumn: '' })).toEqual(false);
  });

  test('metricColumn is empty and aggregationFunction is SUM', () => {
    expect(TimeSeriesBuilder.canRunQuery({ ...params, metricColumn: {}, aggregationFunction: 'SUM' })).toEqual(false);
  });

  test('metricColumn is empty and aggregationFunction is COUNT', () => {
    expect(
      TimeSeriesBuilder.canRunQuery({
        ...params,
        metricColumn: {},
        aggregationFunction: 'COUNT',
      })
    ).toEqual(true);
  });

  test('params are fully populated', () => {
    expect(TimeSeriesBuilder.canRunQuery(params)).toEqual(true);
  });
});

describe('applyDefaults', () => {
  const timeColumns: Column[] = [
    {
      name: 'ts',
      dataType: 'TIMESTAMP',
      key: null,
      isTime: true,
      isDerived: false,
      isMetric: false,
    },
    {
      name: 'ts2',
      dataType: 'TIMESTAMP',
      key: null,
      isTime: true,
      isDerived: false,
      isMetric: false,
    },
  ];
  const metricColumns: Column[] = [
    {
      name: 'met',
      dataType: 'DOUBLE',
      key: null,
      isTime: false,
      isDerived: false,
      isMetric: true,
    },
    {
      name: 'met2',
      dataType: 'DOUBLE',
      key: null,
      isTime: false,
      isDerived: false,
      isMetric: true,
    },
  ];

  test('emptyParams', () => {
    const params = newEmptyParams();
    expect(TimeSeriesBuilder.applyDefaults(params, { timeColumns, metricColumns })).toEqual(true);
    expect(params).toEqual<TimeSeriesBuilder.Params>({
      tableName: '',
      timeColumn: 'ts',
      metricColumn: { name: 'met', key: undefined },
      granularity: '',
      aggregationFunction: 'SUM',
      limit: 0,
      filters: [],
      orderBy: [],
      queryOptions: [],
      legend: '',
      groupByColumns: [],
      seriesLimit: 0
    });
  });

  test('populatedParams', () => {
    const params: TimeSeriesBuilder.Params = {
      tableName: 'test_table',
      timeColumn: 'test_time_column',
      metricColumn: { name: 'test_metric_column', key: 'test_metric_column_key' },
      granularity: 'auto',
      aggregationFunction: 'AVG',
      limit: 100,
      filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
      orderBy: [{ columnName: 'test_order_column', direction: 'asc' }],
      queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
      legend: '{{test_dim_column}}',
      groupByColumns: [{ name: 'test_dim_column' }],
      seriesLimit: 101,
    };
    expect(TimeSeriesBuilder.applyDefaults(params, { timeColumns, metricColumns })).toEqual(false);
    expect(params).toEqual<TimeSeriesBuilder.Params>({
      tableName: 'test_table',
      timeColumn: 'test_time_column',
      metricColumn: { name: 'test_metric_column', key: 'test_metric_column_key' },
      granularity: 'auto',
      aggregationFunction: 'AVG',
      limit: 100,
      filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
      orderBy: [{ columnName: 'test_order_column', direction: 'asc' }],
      queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
      legend: '{{test_dim_column}}',
      groupByColumns: [{ name: 'test_dim_column' }],
      seriesLimit: 101,
    });
  });
});

describe('dataQueryOf', () => {
  const query = { refId: 'test_id' };

  test('params are empty', () => {
    expect(TimeSeriesBuilder.dataQueryOf(query, newEmptyParams())).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: QueryType.PinotQL,
      editorMode: EditorMode.Builder,
      displayType: DisplayType.TIMESERIES,
      tableName: undefined,
      timeColumn: undefined,
      metricColumn: undefined,
      metricColumnV2: undefined,
      granularity: undefined,
      aggregationFunction: undefined,
      limit: undefined,
      filters: undefined,
      orderBy: undefined,
      queryOptions: undefined,
      legend: undefined,
      groupByColumns: undefined,
      groupByColumnsV2: undefined,
    });
  });

  test('params are fully populated', () => {
    expect(
      TimeSeriesBuilder.dataQueryOf(query, {
        tableName: 'test_table',
        timeColumn: 'test_time_column',
        metricColumn: { name: 'test_metric_column', key: 'test_metric_column_key' },
        granularity: 'auto',
        aggregationFunction: 'AVG',
        limit: 100,
        filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
        orderBy: [{ columnName: 'test_order_column', direction: 'asc' }],
        queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
        legend: '{{test_dim_column}}',
        groupByColumns: [{ name: 'test_dim_column' }],
        seriesLimit: 101,
      })
    ).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: QueryType.PinotQL,
      editorMode: EditorMode.Builder,
      displayType: DisplayType.TIMESERIES,
      tableName: 'test_table',
      timeColumn: 'test_time_column',
      metricColumnV2: { name: 'test_metric_column', key: 'test_metric_column_key' },
      granularity: 'auto',
      aggregationFunction: 'AVG',
      limit: 100,
      filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
      orderBy: [{ columnName: 'test_order_column', direction: 'asc' }],
      queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
      legend: '{{test_dim_column}}',
      groupByColumnsV2: [{ name: 'test_dim_column' }],
      seriesLimit: 101,
    });
  });
});

test('resourcesFrom', () => {
  const tablesResult: UseResourceResult<string[]> = { loading: false, result: ['table_1', 'table_2'] };
  const columnsResult: UseResourceResult<Column[]> = {
    loading: false,
    result: [
      {
        name: 'ts',
        dataType: 'TIMESTAMP',
        key: null,
        isTime: true,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'ts2',
        dataType: 'TIMESTAMP',
        key: null,
        isTime: true,
        isDerived: true,
        isMetric: false,
      },
      {
        name: 'met',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'met2',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'dim',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'dim2',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
    ],
  };

  const granularitiesResult: UseResourceResult<Granularity[]> = {
    loading: false,
    result: [{ name: 'SECONDS', optimized: false, seconds: 1 }],
  };

  const sqlPreviewResult: UseResourceResult<string> = {
    loading: false,
    result: 'SELECT * FROM "test_table";',
  };

  const got = TimeSeriesBuilder.resourcesFrom(tablesResult, columnsResult, granularitiesResult, sqlPreviewResult);
  expect(got).toEqual<TimeSeriesBuilder.Resources>({
    tables: ['table_1', 'table_2'],
    isTablesLoading: false,
    columns: [
      {
        name: 'ts',
        dataType: 'TIMESTAMP',
        key: null,
        isTime: true,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'ts2',
        dataType: 'TIMESTAMP',
        key: null,
        isTime: true,
        isDerived: true,
        isMetric: false,
      },
      {
        name: 'met',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'met2',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'dim',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'dim2',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
    ],
    timeColumns: [
      {
        name: 'ts',
        dataType: 'TIMESTAMP',
        key: null,
        isTime: true,
        isDerived: false,
        isMetric: false,
      },
    ],
    metricColumns: [
      {
        name: 'met',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'met2',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
    ],
    groupByColumns: [
      {
        name: 'met',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'met2',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'dim',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'dim2',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
    ],
    filterColumns: [
      {
        name: 'met',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'met2',
        dataType: 'DOUBLE',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: true,
      },
      {
        name: 'dim',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'dim2',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
    ],
    isColumnsLoading: false,
    granularities: [{ name: 'SECONDS', optimized: false, seconds: 1 }],
    isGranularitiesLoading: false,
    sqlPreview: 'SELECT * FROM "test_table";',
    isSqlPreviewLoading: false,
  });
});
