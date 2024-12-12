import {
  applyBuilderDefaults,
  BuilderParams,
  builderParamsFrom,
  canRunBuilderQuery,
  dataQueryWithBuilderParams,
} from './builderParams';
import { Column } from '../resources/columns';
import { PinotDataQuery } from '../types/PinotDataQuery';
import { QueryType } from '../types/QueryType';
import { EditorMode } from '../types/EditorMode';

const newEmptyParams = (): BuilderParams => ({
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
});

describe('builderParamsFrom', () => {
  const query: PinotDataQuery = {
    refId: 'test_id',
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
  };

  test('query is fully populated', () => {
    expect(builderParamsFrom(query)).toEqual<BuilderParams>({
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
    });
  });

  test('aggregationFunction is absent', () => {
    expect(builderParamsFrom({ ...query, aggregationFunction: undefined })).toEqual<BuilderParams>({
      tableName: 'test_table_name',
      timeColumn: 'test_time_column',
      metricColumn: { name: 'test_metric_column2', key: 'test_metric_column_key' },
      granularity: 'auto',
      aggregationFunction: 'SUM',
      limit: 100,
      filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
      orderBy: [{ columnName: 'test_order_column', direction: 'asc' }],
      queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
      legend: '{{test_dim_column}}',
      groupByColumns: [{ name: 'test_dim_column_1' }, { name: 'test_dim_column2', key: 'test_dim_column2_key' }],
    });
  });

  test('metricColumnV2 is absent', () => {
    expect(builderParamsFrom({ ...query, metricColumnV2: undefined })).toEqual<BuilderParams>({
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
    });
  });

  test('query is empty', () => {
    expect(builderParamsFrom({ refId: 'test_id' })).toEqual<BuilderParams>({
      tableName: '',
      timeColumn: '',
      metricColumn: {},
      granularity: '',
      aggregationFunction: 'SUM',
      limit: 0,
      filters: [],
      orderBy: [],
      queryOptions: [],
      legend: '',
      groupByColumns: [],
    });
  });
});

describe('canRunQuery', () => {
  const params: BuilderParams = {
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
  };

  test('params are empty', () => {
    expect(canRunBuilderQuery(newEmptyParams())).toEqual(false);
  });

  test('tableName is empty', () => {
    expect(canRunBuilderQuery({ ...params, tableName: '' })).toEqual(false);
  });

  test('timeColumn is empty', () => {
    expect(canRunBuilderQuery({ ...params, timeColumn: '' })).toEqual(false);
  });

  test('metricColumn is empty and aggregationFunction is SUM', () => {
    expect(canRunBuilderQuery({ ...params, metricColumn: {}, aggregationFunction: 'SUM' })).toEqual(false);
  });

  test('metricColumn is empty and aggregationFunction is COUNT', () => {
    expect(canRunBuilderQuery({ ...params, metricColumn: {}, aggregationFunction: 'COUNT' })).toEqual(true);
  });

  test('params are fully populated', () => {
    expect(canRunBuilderQuery(params)).toEqual(true);
  });
});

describe('applyBuilderDefaults', () => {
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
    applyBuilderDefaults(params, { timeColumns, metricColumns });
    expect(params).toEqual<BuilderParams>({
      tableName: '',
      timeColumn: 'ts',
      metricColumn: { name: 'met', key: undefined },
      granularity: '',
      aggregationFunction: '',
      limit: 0,
      filters: [],
      orderBy: [],
      queryOptions: [],
      legend: '',
      groupByColumns: [],
    });
  });

  test('populatedParams', () => {
    const params: BuilderParams = {
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
    };
    applyBuilderDefaults(params, { timeColumns, metricColumns });
    expect(params).toEqual<BuilderParams>({
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
    });
  });
});

describe('dataQueryWithBuilderParams', () => {
  const query = { refId: 'test_id' };

  test('params are empty', () => {
    expect(dataQueryWithBuilderParams(query, newEmptyParams())).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: QueryType.PinotQL,
      editorMode: EditorMode.Builder,
      tableName: undefined,
      timeColumn: undefined,
      metricColumn: undefined,
      granularity: undefined,
      aggregationFunction: undefined,
      limit: undefined,
      filters: undefined,
      orderBy: undefined,
      queryOptions: undefined,
      legend: undefined,
      groupByColumns: undefined,
    });
  });

  test('params are fully populated', () => {
    expect(
      dataQueryWithBuilderParams(query, {
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
      })
    ).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: QueryType.PinotQL,
      editorMode: EditorMode.Builder,
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
    });
  });
});
