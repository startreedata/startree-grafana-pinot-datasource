import { interpolateVariables, PinotDataQuery } from './PinotDataQuery';
import { setTemplateSrv, TemplateSrv } from '@grafana/runtime';
import { VariableType } from '../components/VariableQueryEditor/SelectVariableType';
import { DisplayType } from './DisplayType';
import { EditorMode } from './EditorMode';
import { PinotDataType } from './PinotDataType';

describe('interpolateVariables', () => {
  afterEach(() => {
    setTemplateSrv(undefined as unknown as TemplateSrv);
  });

  test('emptyQuery', () => {
    expect(interpolateVariables({ refId: 'test_id' })).toEqual<PinotDataQuery>({
      refId: 'test_id',
      aggregationFunction: undefined,
      filters: undefined,
      granularity: undefined,
      groupByColumns: undefined,
      groupByColumnsV2: undefined,
      orderBy: undefined,
      metricColumn: undefined,
      metricColumnV2: undefined,
      logColumn: undefined,
      metadataColumns: undefined,
      jsonExtractors: undefined,
      regexpExtractors: undefined,
      pinotQlCode: undefined,
      promQlCode: undefined,
      queryOptions: undefined,
      timeColumn: undefined,
      variableQuery: undefined,
    });
  });

  test('populatedQuery', () => {
    // Set a mock template service. The actual template service handles interpolation more robustly. This is a simple interpolation for testing.
    setTemplateSrv({
      containsTemplate: () => false,
      getVariables: () => [],
      updateTimeRange: () => {},
      replace: (target?: string) =>
        new Map(
          Object.entries({
            $timeColumn: 'timeColumnReplaced',
            $metricColumn: 'metricColumnReplaced',
            $metricColumnKey: 'metricColumnKeyReplaced',
            $logColumn: 'logColumnReplaced',
            $logColumnKey: 'logColumnKeyReplaced',
            $granularity: 'granularityReplaced',
            $aggFunc: 'aggFuncReplaced',
            $filterColumn: 'filterColumnReplaced',
            $filterColumnKey: 'filterColumnKeyReplaced',
            $filterColumnValue: 'filterColumnValueReplaced',
            $groupByColumn: 'groupByColumnReplaced',
            $groupByColumnKey: 'groupByColumnKeyReplaced',
            $orderByColumn: 'orderByColumnReplaced',
            $orderByColumnKey: 'orderByColumnKeyReplaced',
            $metadataColumn: 'metadataColumnReplaced',
            $metadataColumnKey: 'metadataColumnKeyReplaced',
            $jsonExtractorColumn: 'jsonExtractorColumnReplaced',
            $jsonExtractorKey: 'jsonExtractorKeyReplaced',
            $jsonExtractorAlias: 'jsonExtractorAliasReplaced',
            $regexpExtractorColumn: 'regexpExtractorColumnReplaced',
            $regexpExtractorKey: 'regexpExtractorKeyReplaced',
            $regexpExtractorAlias: 'regexpExtractorAliasReplaced',
            $queryOptionName: 'queryOptionNameReplaced',
            $queryOptionValue: 'queryOptionValueReplaced',
            $pinotQlCode: 'pinotQlCodeReplaced',
            $promQlCode: 'promQlCodeReplaced',
            $variableQueryColumn: 'variableQueryColumnReplaced',
            $variableQueryCode: 'variableQueryCodeReplaced',
          })
        ).get(target || '') || 'no replacement',
    });

    expect(
      interpolateVariables({
        refId: 'test_id',
        displayType: DisplayType.TIMESERIES,
        editorMode: EditorMode.Builder,
        timeColumn: '$timeColumn',
        metricColumn: '$metricColumn',
        metricColumnV2: { name: '$metricColumn', key: '$metricColumnKey' },
        logColumn: { name: '$logColumn', key: '$logColumnKey' },
        aggregationFunction: '$aggFunc',
        filters: [
          {
            columnName: '$filterColumn',
            columnKey: '$filterColumnKey',
            operator: '=',
            valueExprs: ['$filterColumnValue'],
          },
        ],
        granularity: '$granularity',
        groupByColumns: ['$groupByColumn'],
        groupByColumnsV2: [{ name: '$groupByColumn', key: '$groupByColumnKey' }],
        orderBy: [{ columnName: '$orderByColumn', columnKey: '$orderByColumnKey', direction: 'asc' }],
        metadataColumns: [{ name: '$metadataColumn', key: '$metadataColumnKey' }],
        jsonExtractors: [
          {
            source: { name: '$jsonExtractorColumn', key: '$jsonExtractorKey' },
            path: '$.key',
            resultType: PinotDataType.STRING,
            alias: '$jsonExtractorAlias',
          },
        ],
        regexpExtractors: [
          {
            source: { name: '$regexpExtractorColumn', key: '$regexpExtractorKey' },
            pattern: '.*',
            group: 0,
            alias: '$regexpExtractorAlias',
          },
        ],
        queryOptions: [{ name: '$queryOptionName', value: '$queryOptionValue' }],
        pinotQlCode: '$pinotQlCode',
        promQlCode: '$promQlCode',
        variableQuery: {
          variableType: VariableType.PinotQlCode,
          columnName: '$variableQueryColumn',
          pinotQlCode: '$variableQueryCode',
        },
        limit: 100,
        seriesLimit: 200,
      })
    ).toEqual<PinotDataQuery>({
      refId: 'test_id',
      displayType: DisplayType.TIMESERIES,
      editorMode: EditorMode.Builder,
      timeColumn: 'timeColumnReplaced',
      metricColumn: 'metricColumnReplaced',
      metricColumnV2: { name: 'metricColumnReplaced', key: 'metricColumnKeyReplaced' },
      logColumn: { name: 'logColumnReplaced', key: 'logColumnKeyReplaced' },
      aggregationFunction: 'aggFuncReplaced',
      filters: [
        {
          columnName: 'filterColumnReplaced',
          columnKey: 'filterColumnKeyReplaced',
          operator: '=',
          valueExprs: ['filterColumnValueReplaced'],
        },
      ],
      granularity: 'granularityReplaced',
      groupByColumns: ['groupByColumnReplaced'],
      groupByColumnsV2: [{ name: 'groupByColumnReplaced', key: 'groupByColumnKeyReplaced' }],
      orderBy: [{ columnName: 'orderByColumnReplaced', columnKey: 'orderByColumnKeyReplaced', direction: 'asc' }],
      metadataColumns: [{ name: 'metadataColumnReplaced', key: 'metadataColumnKeyReplaced' }],
      jsonExtractors: [
        {
          source: { name: 'jsonExtractorColumnReplaced', key: 'jsonExtractorKeyReplaced' },
          path: '$.key',
          resultType: PinotDataType.STRING,
          alias: 'jsonExtractorAliasReplaced',
        },
      ],
      regexpExtractors: [
        {
          source: { name: 'regexpExtractorColumnReplaced', key: 'regexpExtractorKeyReplaced' },
          pattern: '.*',
          group: 0,
          alias: 'regexpExtractorAliasReplaced',
        },
      ],
      queryOptions: [{ name: 'queryOptionNameReplaced', value: 'queryOptionValueReplaced' }],
      pinotQlCode: 'pinotQlCodeReplaced',
      promQlCode: 'promQlCodeReplaced',
      variableQuery: {
        variableType: VariableType.PinotQlCode,
        columnName: 'variableQueryColumnReplaced',
        pinotQlCode: 'variableQueryCodeReplaced',
      },
      limit: 100,
      seriesLimit: 200,
    });
  });
});
