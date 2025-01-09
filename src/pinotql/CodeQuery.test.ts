import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import * as CodeQuery from './CodeQuery';

const newEmptyParams = (): CodeQuery.Params => {
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

describe('paramsFrom', () => {
  test('query is empty', () => {
    expect(CodeQuery.paramsFrom({ refId: 'test_id' })).toEqual<CodeQuery.Params>({
      displayType: '',
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
      CodeQuery.paramsFrom({
        refId: 'test_id',
        displayType: 'LOGS',
        legend: '{{ dim }}',
        logColumnAlias: 'test_log_column_alias',
        metricColumnAlias: 'test_metric_column_alias',
        pinotQlCode: 'SELECT * FROM "test_table";',
        tableName: 'test_table',
        timeColumnAlias: 'test_time_column_alias',
      })
    ).toEqual<CodeQuery.Params>({
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

describe('applyDefaults', () => {
  test('params are empty', () => {
    const params = newEmptyParams();
    expect(CodeQuery.applyDefaults(params)).toEqual(true);
    expect(params).toEqual<CodeQuery.Params>({
      displayType: 'TIMESERIES',
      legend: '',
      logColumnAlias: '',
      metricColumnAlias: '',
      tableName: '',
      timeColumnAlias: '',
      //language=text
      pinotQlCode: `SELECT $__timeGroup("timestamp") AS $__timeAlias(), SUM("metric") AS $__metricAlias()
FROM $__table()
WHERE $__timeFilter("timestamp")
GROUP BY $__timeGroup("timestamp")
ORDER BY $__timeAlias() DESC
LIMIT 100000`,
    });
  });

  test('params are fully populated', () => {
    const params: CodeQuery.Params = {
      displayType: 'TIMESERIES',
      tableName: 'test_table',
      pinotQlCode: 'SELECT * FROM "test_table";',
      timeColumnAlias: 'test_time_column_alias',
      metricColumnAlias: 'test_metric_column_alias',
      logColumnAlias: 'test_log_column_alias',
      legend: '{{ dim }}',
    };
    expect(CodeQuery.applyDefaults(params)).toEqual(false);
    expect(params).toEqual<CodeQuery.Params>({
      displayType: 'TIMESERIES',
      tableName: 'test_table',
      pinotQlCode: 'SELECT * FROM "test_table";',
      timeColumnAlias: 'test_time_column_alias',
      metricColumnAlias: 'test_metric_column_alias',
      logColumnAlias: 'test_log_column_alias',
      legend: '{{ dim }}',
    });
  });
});

describe('dataQueryOf', () => {
  test('params are empty', () => {
    expect(CodeQuery.dataQueryOf({ refId: 'test_id' }, newEmptyParams())).toEqual<PinotDataQuery>({
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
    const params: CodeQuery.Params = {
      displayType: 'LOGS',
      tableName: 'test_table',
      pinotQlCode: 'SELECT * FROM "test_table";',
      timeColumnAlias: 'test_time_column_alias',
      metricColumnAlias: 'test_metric_column_alias',
      logColumnAlias: 'test_log_column_alias',
      legend: '{{ dim }}',
    };

    expect(CodeQuery.dataQueryOf({ refId: 'test_id' }, params)).toEqual<PinotDataQuery>({
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
