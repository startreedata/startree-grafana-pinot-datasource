import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import * as CodeQuery from './CodeQuery';
import { TimeSeriesBuilder } from './index';

const newEmptyParams = (): CodeQuery.Params => {
  return {
    displayType: '',
    tableName: '',
    pinotQlCode: '',
    timeColumnAlias: '',
    metricColumnAlias: '',
    logColumnAlias: '',
    legend: '',
    seriesLimit: 0,
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
      seriesLimit: 0,
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
        seriesLimit: 101,
      })
    ).toEqual<CodeQuery.Params>({
      displayType: 'LOGS',
      legend: '{{ dim }}',
      logColumnAlias: 'test_log_column_alias',
      metricColumnAlias: 'test_metric_column_alias',
      pinotQlCode: 'SELECT * FROM "test_table";',
      tableName: 'test_table',
      timeColumnAlias: 'test_time_column_alias',
      seriesLimit: 101,
    });
  });
});

describe('paramsFromTimeSeriesBuilder', () => {
  const params: TimeSeriesBuilder.Params = {
    tableName: 'test_table',
    timeColumn: 'test_time_column',
    metricColumn: { name: 'test_metric', key: 'test_metric_key' },
    legend: '{{ dim }}',

    // These fields are not used in the function.
    aggregationFunction: '',
    filters: [],
    granularity: '',
    groupByColumns: [],
    limit: 0,
    orderBy: [],
    queryOptions: [],
  };

  expect(CodeQuery.paramsFromTimeSeriesBuilder(params, 'SELECT * FROM "test_table";')).toEqual<CodeQuery.Params>({
    displayType: 'TIMESERIES',
    legend: '{{ dim }}',
    logColumnAlias: '',
    metricColumnAlias: "test_metric['test_metric_key']",
    pinotQlCode: 'SELECT * FROM "test_table";',
    tableName: 'test_table',
    timeColumnAlias: '',
  });
});

describe('paramsFromLogsBuilder', () => {
  expect(
    CodeQuery.paramsFromLogsBuilder(
      {
        tableName: 'test_table',
        logColumn: { name: 'test_log', key: 'test_log_key' },

        // These fields are not used in the function.
        filters: [],
        jsonExtractors: [],
        limit: 0,
        metadataColumns: [],
        queryOptions: [],
        regexpExtractors: [],
        timeColumn: '',
      },
      'SELECT * FROM "test_table";'
    )
  ).toEqual<CodeQuery.Params>({
    displayType: 'LOGS',
    logColumnAlias: "test_log['test_log_key']",
    metricColumnAlias: '',
    pinotQlCode: 'SELECT * FROM "test_table";',
    tableName: 'test_table',
    legend: '',
    timeColumnAlias: '',
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
      pinotQlCode: `SELECT $__timeGroup("timestamp") AS $__timeAlias()
FROM $__table()
WHERE $__timeFilter("timestamp")
GROUP BY $__timeGroup("timestamp")
ORDER BY $__timeAlias() DESC
LIMIT 100000`,
      seriesLimit: 0,
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
      seriesLimit: 101,
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
      seriesLimit: 101,
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
      seriesLimit: undefined,
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
      seriesLimit: 101,
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
      seriesLimit: 101,
    });
  });
});
