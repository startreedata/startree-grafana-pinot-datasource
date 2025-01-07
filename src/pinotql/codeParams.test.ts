import { CodeParams, codeParamsFrom, dataQueryWithCodeParams } from './codeParams';
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
      displayType: 'TIMESERIES',
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
        displayType: 'LOGS',
        legend: '{{ dim }}',
        logColumnAlias: 'test_log_column_alias',
        metricColumnAlias: 'test_metric_column_alias',
        pinotQlCode: 'SELECT * FROM "test_table";',
        tableName: 'test_table',
        timeColumnAlias: 'test_time_column_alias',
      })
    ).toEqual<CodeParams>({
      displayType: 'LOGS',
      legend: '{{ dim }}',
      logColumnAlias: 'test_log_column_alias',
      metricColumnAlias: 'test_metric_column_alias',
      pinotQlCode: 'SELECT * FROM "test_table";',
      tableName: 'test_table',
      timeColumnAlias: 'test_time_column_alias',
    });
  });
});

describe('dataQueryWithCodeParams', () => {
  test('params are empty', () => {
    expect(dataQueryWithCodeParams({ refId: 'test_id' }, newEmptyParams())).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: 'PinotQL',
      editorMode: 'Code',
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
      displayType: 'LOGS',
      tableName: 'test_table',
      pinotQlCode: 'SELECT * FROM "test_table";',
      timeColumnAlias: 'test_time_column_alias',
      metricColumnAlias: 'test_metric_column_alias',
      logColumnAlias: 'test_log_column_alias',
      legend: '{{ dim }}',
    };

    expect(dataQueryWithCodeParams({ refId: 'test_id' }, params)).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: 'PinotQL',
      editorMode: 'Code',
      displayType: 'LOGS',
      tableName: 'test_table',
      pinotQlCode: 'SELECT * FROM "test_table";',
      timeColumnAlias: 'test_time_column_alias',
      metricColumnAlias: 'test_metric_column_alias',
      logColumnAlias: 'test_log_column_alias',
      legend: '{{ dim }}',
    });
  });
});
