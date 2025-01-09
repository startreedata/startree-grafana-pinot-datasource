import * as LogsBuilder from './LogsBuilder';
import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { Column } from '../resources/columns';
import { QueryType } from '../dataquery/QueryType';
import { EditorMode } from '../dataquery/EditorMode';
import { UseResourceResult } from '../resources/UseResourceResult';
import { DisplayType } from '../dataquery/DisplayType';

const newEmptyParams = (): LogsBuilder.Params => ({
  tableName: '',
  timeColumn: '',
  logColumn: {},
  limit: 0,
  filters: [],
  queryOptions: [],
  metadataColumns: [],
  jsonExtractors: [],
  regexpExtractors: [],
});

describe('paramsFrom', () => {
  const query: PinotDataQuery = {
    refId: 'test_id',
    tableName: 'test_table_name',
    timeColumn: 'test_time_column',
    logColumn: { name: 'test_log_column', key: 'test_metric_column_key' },
    limit: 100,
    filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
    queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
    metadataColumns: [{ name: 'metadata_column', key: 'metadata_column_key' }],
    regexpExtractors: [
      {
        source: { name: 'regex_column', key: 'regex_column_key' },
        pattern: '(.*)',
        group: 1,
        alias: 'regex_extracted',
      },
    ],

    jsonExtractors: [
      {
        source: { name: 'regex_column', key: 'regex_column_key' },
        path: '$.key',
        resultType: 'STRING',
        alias: 'json_extracted',
      },
    ],
  };

  test('query is fully populated', () => {
    expect(LogsBuilder.paramsFrom(query)).toEqual<LogsBuilder.Params>({
      tableName: 'test_table_name',
      timeColumn: 'test_time_column',
      logColumn: { name: 'test_log_column', key: 'test_metric_column_key' },
      limit: 100,
      filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
      queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
      metadataColumns: [{ name: 'metadata_column', key: 'metadata_column_key' }],
      regexpExtractors: [
        {
          source: { name: 'regex_column', key: 'regex_column_key' },
          pattern: '(.*)',
          group: 1,
          alias: 'regex_extracted',
        },
      ],

      jsonExtractors: [
        {
          source: { name: 'regex_column', key: 'regex_column_key' },
          path: '$.key',
          resultType: 'STRING',
          alias: 'json_extracted',
        },
      ],
    });
  });

  test('query is empty', () => {
    expect(LogsBuilder.paramsFrom({ refId: 'test_id' })).toEqual<LogsBuilder.Params>({
      tableName: '',
      timeColumn: '',
      logColumn: {},
      limit: 0,
      filters: [],
      queryOptions: [],
      metadataColumns: [],
      jsonExtractors: [],
      regexpExtractors: [],
    });
  });
});

describe('canRunQuery', () => {
  const params: LogsBuilder.Params = {
    tableName: 'test_table_name',
    timeColumn: 'test_time_column',
    logColumn: { name: 'test_log_column', key: 'test_metric_column_key' },
    limit: 100,
    filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
    queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
    metadataColumns: [{ name: 'metadata_column', key: 'metadata_column_key' }],
    regexpExtractors: [
      {
        source: { name: 'regex_column', key: 'regex_column_key' },
        pattern: '(.*)',
        group: 1,
        alias: 'regex_extracted',
      },
    ],

    jsonExtractors: [
      {
        source: { name: 'regex_column', key: 'regex_column_key' },
        path: '$.key',
        resultType: 'STRING',
        alias: 'json_extracted',
      },
    ],
  };

  test('params are empty', () => {
    expect(LogsBuilder.canRunQuery(newEmptyParams())).toEqual(false);
  });

  test('tableName is empty', () => {
    expect(LogsBuilder.canRunQuery({ ...params, tableName: '' })).toEqual(false);
  });

  test('timeColumn is empty', () => {
    expect(LogsBuilder.canRunQuery({ ...params, timeColumn: '' })).toEqual(false);
  });

  test('logColumn is empty', () => {
    expect(LogsBuilder.canRunQuery({ ...params, logColumn: {} })).toEqual(false);
  });

  test('params are fully populated', () => {
    expect(LogsBuilder.canRunQuery(params)).toEqual(true);
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
  const logMessageColumns: Column[] = [
    {
      name: 'message1',
      dataType: 'STRING',
      key: null,
      isTime: false,
      isDerived: false,
      isMetric: true,
    },
    {
      name: 'message2',
      dataType: 'STRING',
      key: null,
      isTime: false,
      isDerived: false,
      isMetric: true,
    },
  ];

  test('emptyParams', () => {
    const params = newEmptyParams();
    expect(LogsBuilder.applyDefaults(params, { timeColumns, logMessageColumns })).toEqual(true);
    expect(params).toEqual<LogsBuilder.Params>({
      tableName: '',
      timeColumn: 'ts',
      logColumn: { name: 'message1', key: undefined },
      limit: 0,
      filters: [],
      queryOptions: [],
      metadataColumns: [],
      jsonExtractors: [],
      regexpExtractors: [],
    });
  });

  test('populatedParams', () => {
    const params: LogsBuilder.Params = {
      tableName: 'test_table_name',
      timeColumn: 'test_time_column',
      logColumn: { name: 'test_log_column', key: 'test_metric_column_key' },
      limit: 100,
      filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
      queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
      metadataColumns: [{ name: 'metadata_column', key: 'metadata_column_key' }],
      regexpExtractors: [
        {
          source: { name: 'regex_column', key: 'regex_column_key' },
          pattern: '(.*)',
          group: 1,
          alias: 'regex_extracted',
        },
      ],

      jsonExtractors: [
        {
          source: { name: 'regex_column', key: 'regex_column_key' },
          path: '$.key',
          resultType: 'STRING',
          alias: 'json_extracted',
        },
      ],
    };
    expect(LogsBuilder.applyDefaults(params, { timeColumns, logMessageColumns })).toEqual(false);
    expect(params).toEqual<LogsBuilder.Params>({
      tableName: 'test_table_name',
      timeColumn: 'test_time_column',
      logColumn: { name: 'test_log_column', key: 'test_metric_column_key' },
      limit: 100,
      filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
      queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
      metadataColumns: [{ name: 'metadata_column', key: 'metadata_column_key' }],
      regexpExtractors: [
        {
          source: { name: 'regex_column', key: 'regex_column_key' },
          pattern: '(.*)',
          group: 1,
          alias: 'regex_extracted',
        },
      ],

      jsonExtractors: [
        {
          source: { name: 'regex_column', key: 'regex_column_key' },
          path: '$.key',
          resultType: 'STRING',
          alias: 'json_extracted',
        },
      ],
    });
  });
});

describe('dataQueryOf', () => {
  const query = { refId: 'test_id' };

  test('params are empty', () => {
    expect(LogsBuilder.dataQueryOf(query, newEmptyParams())).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: QueryType.PinotQL,
      editorMode: EditorMode.Builder,
      displayType: DisplayType.LOGS,
      tableName: undefined,
      timeColumn: undefined,
      logColumn: undefined,
      limit: undefined,
      filters: undefined,
      queryOptions: undefined,
      metadataColumns: undefined,
      jsonExtractors: undefined,
      regexpExtractors: undefined,
    });
  });

  test('params are fully populated', () => {
    expect(
      LogsBuilder.dataQueryOf(query, {
        tableName: 'test_table_name',
        timeColumn: 'test_time_column',
        logColumn: { name: 'test_log_column', key: 'test_metric_column_key' },
        limit: 100,
        filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
        queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
        metadataColumns: [{ name: 'metadata_column', key: 'metadata_column_key' }],
        regexpExtractors: [
          {
            source: { name: 'regex_column', key: 'regex_column_key' },
            pattern: '(.*)',
            group: 1,
            alias: 'regex_extracted',
          },
        ],

        jsonExtractors: [
          {
            source: { name: 'regex_column', key: 'regex_column_key' },
            path: '$.key',
            resultType: 'STRING',
            alias: 'json_extracted',
          },
        ],
      })
    ).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: QueryType.PinotQL,
      editorMode: EditorMode.Builder,
      displayType: DisplayType.LOGS,
      tableName: 'test_table_name',
      timeColumn: 'test_time_column',
      logColumn: { name: 'test_log_column', key: 'test_metric_column_key' },
      limit: 100,
      filters: [{ columnName: 'test_filter_column', operator: '=', valueExprs: ['test_value'] }],
      queryOptions: [{ name: 'test_query_option', value: 'test_option_value' }],
      metadataColumns: [{ name: 'metadata_column', key: 'metadata_column_key' }],
      regexpExtractors: [
        {
          source: { name: 'regex_column', key: 'regex_column_key' },
          pattern: '(.*)',
          group: 1,
          alias: 'regex_extracted',
        },
      ],

      jsonExtractors: [
        {
          source: { name: 'regex_column', key: 'regex_column_key' },
          path: '$.key',
          resultType: 'STRING',
          alias: 'json_extracted',
        },
      ],
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
        name: 'string1',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'string2',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
    ],
  };

  const sqlPreviewResult: UseResourceResult<string> = {
    loading: false,
    result: 'SELECT * FROM "test_table";',
  };

  const got = LogsBuilder.resourcesFrom(tablesResult, columnsResult, sqlPreviewResult);
  expect(got).toEqual<LogsBuilder.Resources>({
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
        name: 'string1',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'string2',
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
    logMessageColumns: [
      {
        name: 'string1',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'string2',
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
        name: 'string1',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
      {
        name: 'string2',
        dataType: 'STRING',
        key: null,
        isTime: false,
        isDerived: false,
        isMetric: false,
      },
    ],
    isColumnsLoading: false,
    sqlPreview: 'SELECT * FROM "test_table";',
    isSqlPreviewLoading: false,
  });
});
