import { applyConditionalAll, interpolateVariables, PinotDataQuery } from './PinotDataQuery';
import { setTemplateSrv, TemplateSrv } from '@grafana/runtime';
import { TypedVariableModel } from '@grafana/data';
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

  test('attachesAdHocFilters', () => {
    setTemplateSrv({
      containsTemplate: () => false,
      getVariables: () => [],
      updateTimeRange: () => {},
      replace: (target?: string) => target ?? '',
    } as unknown as TemplateSrv);

    const filters = [{ key: 'city', operator: '=', value: 'NY' }];
    const result = interpolateVariables({ refId: 'A', tableName: 't' }, {}, filters);

    expect(result.adHocFilters).toEqual(filters);
  });

  test('noAdHocFiltersLeavesFieldUndefined', () => {
    expect(interpolateVariables({ refId: 'A' }).adHocFilters).toBeUndefined();
  });

  test('interpolatesTableName', () => {
    // Enables binding a panel's table to a `$table` template variable (and chained column variables).
    setTemplateSrv({
      containsTemplate: () => false,
      getVariables: () => [],
      updateTimeRange: () => {},
      replace: (target?: string) => (target === '$table' ? 'realTable' : target ?? ''),
    } as unknown as TemplateSrv);

    expect(interpolateVariables({ refId: 'A', tableName: '$table' }).tableName).toBe('realTable');
  });
});

describe('applyConditionalAll', () => {
  const variable = (name: string, value: string | string[]): TypedVariableModel =>
    ({ name, current: { value } } as unknown as TypedVariableModel);

  test('drops the condition to 1=1 when "All" is selected (scalar)', () => {
    const sql = "WHERE $__conditionalAll(service = '$service', $service)";
    expect(applyConditionalAll(sql, [variable('service', '$__all')])).toBe('WHERE 1=1');
  });

  test('drops the condition when "All" is selected in a multi-value array', () => {
    const sql = 'WHERE $__conditionalAll(x = 1, $service)';
    expect(applyConditionalAll(sql, [variable('service', ['$__all'])])).toBe('WHERE 1=1');
  });

  test('drops the condition when the variable has no selection', () => {
    const sql = 'WHERE $__conditionalAll(x = 1, $service)';
    expect(applyConditionalAll(sql, [variable('service', '')])).toBe('WHERE 1=1');
    expect(applyConditionalAll(sql, [variable('service', [])])).toBe('WHERE 1=1');
  });

  test('keeps the condition when a concrete value is selected', () => {
    const sql = "WHERE $__conditionalAll(service = '$service', $service)";
    expect(applyConditionalAll(sql, [variable('service', 'checkout')])).toBe("WHERE service = '$service'");
  });

  test('keeps the condition when the variable is not found', () => {
    const sql = 'WHERE $__conditionalAll(x = 1, $missing)';
    expect(applyConditionalAll(sql, [])).toBe('WHERE x = 1');
  });

  test('handles conditions containing commas and nested parens', () => {
    const sql = "WHERE $__conditionalAll(service IN ('a', 'b') AND f(x, y) > 0, $service)";
    expect(applyConditionalAll(sql, [variable('service', 'a')])).toBe(
      "WHERE service IN ('a', 'b') AND f(x, y) > 0"
    );
    expect(applyConditionalAll(sql, [variable('service', '$__all')])).toBe('WHERE 1=1');
  });

  test('expands multiple macros independently', () => {
    const sql = '$__conditionalAll(a = 1, $a) AND $__conditionalAll(b = 2, $b)';
    const vars = [variable('a', '$__all'), variable('b', 'x')];
    expect(applyConditionalAll(sql, vars)).toBe('1=1 AND b = 2');
  });

  test('accepts the ${var} reference form', () => {
    const sql = 'WHERE $__conditionalAll(x = 1, ${service})';
    expect(applyConditionalAll(sql, [variable('service', '$__all')])).toBe('WHERE 1=1');
  });

  test('leaves malformed (non two-argument) invocations untouched', () => {
    const sql = 'WHERE $__conditionalAll(x = 1)';
    expect(applyConditionalAll(sql, [variable('service', '$__all')])).toBe(sql);
  });

  test('returns the query unchanged when no macro is present', () => {
    const sql = 'SELECT * FROM t WHERE x = 1';
    expect(applyConditionalAll(sql, [variable('service', '$__all')])).toBe(sql);
  });
});

describe('interpolateVariables with $__conditionalAll', () => {
  afterEach(() => {
    setTemplateSrv(undefined as unknown as TemplateSrv);
  });

  // Wires a mock template service whose getVariables() exposes `service` and replace() substitutes
  // `$service` -> 'checkout'. This exercises the full code-mode path: $__conditionalAll runs first,
  // then templateSrv.replace interpolates whatever condition survived.
  const setSrv = (current: string | string[]) => {
    setTemplateSrv({
      containsTemplate: () => false,
      updateTimeRange: () => {},
      getVariables: () =>
        [{ name: 'service', type: 'query', current: { value: current } }] as unknown as ReturnType<
          TemplateSrv['getVariables']
        >,
      replace: (target?: string) => (target ?? '').replace(/\$service/g, 'checkout'),
    } as unknown as TemplateSrv);
  };

  test('drops the filter and still interpolates the rest when All is selected', () => {
    setSrv('$__all');
    const query: PinotDataQuery = {
      refId: 'A',
      editorMode: EditorMode.Code,
      pinotQlCode: "SELECT * FROM t WHERE $__conditionalAll(service = '$service', $service) AND env = '$service'",
    };
    expect(interpolateVariables(query).pinotQlCode).toBe("SELECT * FROM t WHERE 1=1 AND env = 'checkout'");
  });

  test('keeps the filter and interpolates the variable when a value is selected', () => {
    setSrv('checkout');
    const query: PinotDataQuery = {
      refId: 'A',
      editorMode: EditorMode.Code,
      pinotQlCode: "SELECT * FROM t WHERE $__conditionalAll(service = '$service', $service)",
    };
    expect(interpolateVariables(query).pinotQlCode).toBe("SELECT * FROM t WHERE service = 'checkout'");
  });
});
