import { canRunCodeQuery, CodeParams, codeParamsFrom, dataQueryWithCodeParams } from './codeParams';
import { DisplayTypeLogs, DisplayTypeTimeSeries } from '../components/QueryEditor/SelectDisplayType';
import { PinotDataQuery } from '../dataquery/PinotDataQuery';

const newEmptyParams = (): CodeParams => {
  return {
    displayType: '',
    tableName: '',
    pinotQlCode: '',
    timeColumnAlias: '',
    metricColumnAlias: '',
    logColumnAlias: '',
    legend: '',
  };
};

describe('codeParamsFrom', () => {
  test('query is empty', () => {
    expect(codeParamsFrom({ refId: 'test_id' })).toEqual<CodeParams>({
      displayType: DisplayTypeTimeSeries,
      legend: '',
      logColumnAlias: '',
      metricColumnAlias: '',
      pinotQlCode: '',
      tableName: '',
      timeColumnAlias: '',
    });
  });

  test('query is fully populated', () => {
    expect(
      codeParamsFrom({
        refId: 'test_id',
        displayType: DisplayTypeLogs,
        legend: '{{ dim }}',
        logColumnAlias: 'test_log_column_alias',
        metricColumnAlias: 'test_metric_column_alias',
        pinotQlCode: 'SELECT * FROM "test_table";',
        tableName: 'test_table',
        timeColumnAlias: 'test_time_column_alias',
      })
    ).toEqual<CodeParams>({
      displayType: DisplayTypeLogs,
      legend: '{{ dim }}',
      logColumnAlias: 'test_log_column_alias',
      metricColumnAlias: 'test_metric_column_alias',
      pinotQlCode: 'SELECT * FROM "test_table";',
      tableName: 'test_table',
      timeColumnAlias: 'test_time_column_alias',
    });
  });
});

describe('canRunCodeQuery', () => {
  const params: CodeParams = {
    displayType: DisplayTypeLogs,
    legend: '{{ dim }}',
    logColumnAlias: 'test_log_column_alias',
    metricColumnAlias: 'test_metric_column_alias',
    pinotQlCode: 'SELECT * FROM "test_table";',
    tableName: 'test_table',
    timeColumnAlias: 'test_time_column_alias',
  };

  test('params are empty', () => {
    expect(canRunCodeQuery(newEmptyParams())).toEqual(false);
  });

  test('tableName is empty', () => {
    expect(canRunCodeQuery({ ...params, tableName: '' })).toEqual(false);
  });

  test('pinotQlCode is empty', () => {
    expect(canRunCodeQuery({ ...params, pinotQlCode: '' })).toEqual(false);
  });

  test('params are fully populated', () => {
    expect(canRunCodeQuery(params)).toEqual(true);
  });
});

describe('dataQueryWithCodeParams', () => {
  test('params are empty', () => {
    expect(dataQueryWithCodeParams({ refId: 'test_id' }, newEmptyParams())).toEqual<PinotDataQuery>({
      refId: 'test_id',
      displayType: undefined,
      tableName: undefined,
      pinotQlCode: undefined,
      timeColumnAlias: undefined,
      metricColumnAlias: undefined,
      logColumnAlias: undefined,
      legend: undefined,
    });
  });

  test('params are fully populated', () => {
    const params: CodeParams = {
      displayType: DisplayTypeLogs,
      tableName: 'test_table',
      pinotQlCode: 'SELECT * FROM "test_table";',
      timeColumnAlias: 'test_time_column_alias',
      metricColumnAlias: 'test_metric_column_alias',
      logColumnAlias: 'test_log_column_alias',
      legend: '{{ dim }}',
    };

    expect(dataQueryWithCodeParams({ refId: 'test_id' }, params)).toEqual<PinotDataQuery>({
      refId: 'test_id',
      displayType: DisplayTypeLogs,
      tableName: 'test_table',
      pinotQlCode: 'SELECT * FROM "test_table";',
      timeColumnAlias: 'test_time_column_alias',
      metricColumnAlias: 'test_metric_column_alias',
      logColumnAlias: 'test_log_column_alias',
      legend: '{{ dim }}',
    });
  });
});
