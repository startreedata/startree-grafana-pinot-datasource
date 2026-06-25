import * as TracesBuilder from './TracesBuilder';
import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { Column } from '../resources/columns';
import { QueryType } from '../dataquery/QueryType';
import { EditorMode } from '../dataquery/EditorMode';
import { DisplayType } from '../dataquery/DisplayType';

const newEmptyParams = (): TracesBuilder.Params => ({
  tableName: '',
  timeColumn: '',
  traceIdColumn: {},
  spanIdColumn: {},
  parentSpanIdColumn: {},
  serviceNameColumn: {},
  spanNameColumn: {},
  durationColumn: {},
  durationUnit: 'ms',
  tagsColumn: {},
  statusColumn: {},
  traceId: '',
  limit: 0,
  filters: [],
  queryOptions: [],
});

const newFullParams = (): TracesBuilder.Params => ({
  tableName: 'otelTraces',
  timeColumn: 'ts',
  traceIdColumn: { name: 'trace_id' },
  spanIdColumn: { name: 'span_id' },
  parentSpanIdColumn: { name: 'parent_span_id' },
  serviceNameColumn: { name: 'service_name' },
  spanNameColumn: { name: 'name' },
  durationColumn: { name: 'duration_ns' },
  durationUnit: 'ns',
  tagsColumn: { name: 'tags' },
  statusColumn: { name: 'status_code' },
  traceId: 'abc123',
  limit: 50,
  filters: [{ columnName: 'service_name', operator: '=', valueExprs: ["'frontend'"] }],
  queryOptions: [{ name: 'timeoutMs', value: '1000' }],
});

const column = (name: string, dataType: string, overrides: Partial<Column> = {}): Column => ({
  name,
  dataType,
  key: null,
  isTime: false,
  isDerived: false,
  isMetric: false,
  ...overrides,
});

describe('paramsFrom', () => {
  test('query is fully populated', () => {
    const query: PinotDataQuery = {
      refId: 'test_id',
      tableName: 'otelTraces',
      timeColumn: 'ts',
      traceIdColumn: { name: 'trace_id' },
      spanIdColumn: { name: 'span_id' },
      parentSpanIdColumn: { name: 'parent_span_id' },
      serviceNameColumn: { name: 'service_name' },
      spanNameColumn: { name: 'name' },
      durationColumn: { name: 'duration_ns' },
      durationUnit: 'ns',
      tagsColumn: { name: 'tags' },
      statusColumn: { name: 'status_code' },
      traceId: 'abc123',
      limit: 50,
      filters: [{ columnName: 'service_name', operator: '=', valueExprs: ["'frontend'"] }],
      queryOptions: [{ name: 'timeoutMs', value: '1000' }],
    };
    expect(TracesBuilder.paramsFrom(query)).toEqual<TracesBuilder.Params>(newFullParams());
  });

  test('query is empty defaults durationUnit to ms', () => {
    expect(TracesBuilder.paramsFrom({ refId: 'test_id' })).toEqual<TracesBuilder.Params>(newEmptyParams());
  });
});

describe('canRunQuery', () => {
  test('params are empty', () => {
    expect(TracesBuilder.canRunQuery(newEmptyParams())).toEqual(false);
  });
  test('missing table', () => {
    expect(TracesBuilder.canRunQuery({ ...newFullParams(), tableName: '' })).toEqual(false);
  });
  test('missing time column', () => {
    expect(TracesBuilder.canRunQuery({ ...newFullParams(), timeColumn: '' })).toEqual(false);
  });
  test('missing trace id column', () => {
    expect(TracesBuilder.canRunQuery({ ...newFullParams(), traceIdColumn: {} })).toEqual(false);
  });
  test('missing span id column', () => {
    expect(TracesBuilder.canRunQuery({ ...newFullParams(), spanIdColumn: {} })).toEqual(false);
  });
  test('missing duration column', () => {
    expect(TracesBuilder.canRunQuery({ ...newFullParams(), durationColumn: {} })).toEqual(false);
  });
  test('optional columns not required', () => {
    expect(
      TracesBuilder.canRunQuery({
        ...newFullParams(),
        parentSpanIdColumn: {},
        serviceNameColumn: {},
        spanNameColumn: {},
        tagsColumn: {},
        statusColumn: {},
      })
    ).toEqual(true);
  });
  test('params fully populated', () => {
    expect(TracesBuilder.canRunQuery(newFullParams())).toEqual(true);
  });
});

describe('otelDefaults', () => {
  test('fills all trace roles with OTel column names', () => {
    const params = newEmptyParams();
    expect(TracesBuilder.otelDefaults(params)).toEqual<TracesBuilder.Params>({
      ...newEmptyParams(),
      traceIdColumn: { name: 'trace_id' },
      spanIdColumn: { name: 'span_id' },
      parentSpanIdColumn: { name: 'parent_span_id' },
      serviceNameColumn: { name: 'service_name' },
      spanNameColumn: { name: 'name' },
      durationColumn: { name: 'duration_nano' },
      durationUnit: 'ns',
      tagsColumn: { name: 'span_attributes' },
      statusColumn: { name: 'status_code' },
    });
  });

  test('overwrites existing column mappings but preserves non-column fields', () => {
    const params: TracesBuilder.Params = {
      ...newFullParams(),
      tableName: 'my_spans',
      timeColumn: 'event_time',
      traceId: 'keep-me',
      limit: 25,
    };
    const got = TracesBuilder.otelDefaults(params);

    // Column roles are overwritten with the OTel preset.
    expect(got.traceIdColumn).toEqual({ name: 'trace_id' });
    expect(got.durationColumn).toEqual({ name: 'duration_nano' });
    expect(got.durationUnit).toEqual('ns');

    // Non-column fields are untouched.
    expect(got.tableName).toEqual('my_spans');
    expect(got.timeColumn).toEqual('event_time');
    expect(got.traceId).toEqual('keep-me');
    expect(got.limit).toEqual(25);
  });

  test('does not mutate the input params', () => {
    const params = newEmptyParams();
    TracesBuilder.otelDefaults(params);
    expect(params).toEqual<TracesBuilder.Params>(newEmptyParams());
  });
});

describe('applyDefaults', () => {
  const timeColumns: Column[] = [column('ts', 'TIMESTAMP', { isTime: true })];
  const columns: Column[] = [
    column('ts', 'TIMESTAMP', { isTime: true }),
    column('traceId', 'STRING'),
    column('spanId', 'STRING'),
    column('parentSpanId', 'STRING'),
    column('serviceName', 'STRING'),
    column('name', 'STRING'),
    column('durationMs', 'LONG', { isMetric: true }),
    column('tags', 'JSON'),
    column('statusCode', 'STRING'),
  ];

  test('auto-maps OTel columns and time column', () => {
    const params = newEmptyParams();
    expect(TracesBuilder.applyDefaults(params, { timeColumns, columns })).toEqual(true);
    expect(params).toEqual<TracesBuilder.Params>({
      ...newEmptyParams(),
      timeColumn: 'ts',
      traceIdColumn: { name: 'traceId', key: undefined },
      spanIdColumn: { name: 'spanId', key: undefined },
      parentSpanIdColumn: { name: 'parentSpanId', key: undefined },
      serviceNameColumn: { name: 'serviceName', key: undefined },
      spanNameColumn: { name: 'name', key: undefined },
      durationColumn: { name: 'durationMs', key: undefined },
      tagsColumn: { name: 'tags', key: undefined },
      statusColumn: { name: 'statusCode', key: undefined },
    });
  });

  test('does not overwrite populated params', () => {
    const params = newFullParams();
    expect(TracesBuilder.applyDefaults(params, { timeColumns, columns })).toEqual(false);
    expect(params).toEqual<TracesBuilder.Params>(newFullParams());
  });
});

describe('dataQueryOf', () => {
  const query = { refId: 'test_id' };

  test('params are empty', () => {
    expect(TracesBuilder.dataQueryOf(query, newEmptyParams())).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: QueryType.PinotQL,
      editorMode: EditorMode.Builder,
      displayType: DisplayType.TRACES,
      tableName: undefined,
      timeColumn: undefined,
      traceIdColumn: undefined,
      spanIdColumn: undefined,
      parentSpanIdColumn: undefined,
      serviceNameColumn: undefined,
      spanNameColumn: undefined,
      durationColumn: undefined,
      durationUnit: 'ms',
      tagsColumn: undefined,
      statusColumn: undefined,
      traceId: undefined,
      limit: undefined,
      filters: undefined,
      queryOptions: undefined,
    });
  });

  test('params fully populated', () => {
    expect(TracesBuilder.dataQueryOf(query, newFullParams())).toEqual<PinotDataQuery>({
      refId: 'test_id',
      queryType: QueryType.PinotQL,
      editorMode: EditorMode.Builder,
      displayType: DisplayType.TRACES,
      tableName: 'otelTraces',
      timeColumn: 'ts',
      traceIdColumn: { name: 'trace_id' },
      spanIdColumn: { name: 'span_id' },
      parentSpanIdColumn: { name: 'parent_span_id' },
      serviceNameColumn: { name: 'service_name' },
      spanNameColumn: { name: 'name' },
      durationColumn: { name: 'duration_ns' },
      durationUnit: 'ns',
      tagsColumn: { name: 'tags' },
      statusColumn: { name: 'status_code' },
      traceId: 'abc123',
      limit: 50,
      filters: [{ columnName: 'service_name', operator: '=', valueExprs: ["'frontend'"] }],
      queryOptions: [{ name: 'timeoutMs', value: '1000' }],
    });
  });
});

test('resourcesFrom categorizes columns by role', () => {
  const columns: Column[] = [
    column('ts', 'TIMESTAMP', { isTime: true }),
    column('tsDerived', 'TIMESTAMP', { isTime: true, isDerived: true }),
    column('trace_id', 'STRING'),
    column('duration_ns', 'LONG', { isMetric: true }),
    column('tags', 'JSON'),
    column('attrs', 'MAP'),
  ];
  const got = TracesBuilder.resourcesFrom(
    { loading: false, result: ['otelTraces'] },
    { loading: false, result: columns },
    { loading: false, result: 'SELECT 1;' }
  );

  expect(got.timeColumns.map((c) => c.name)).toEqual(['ts']);
  expect(got.stringColumns.map((c) => c.name)).toEqual(['trace_id']);
  expect(got.numericColumns.map((c) => c.name)).toEqual(['duration_ns']);
  expect(got.tagsColumns.map((c) => c.name)).toEqual(['tags', 'attrs']);
  expect(got.filterColumns.map((c) => c.name)).toEqual(['trace_id', 'duration_ns', 'tags', 'attrs']);
  expect(got.sqlPreview).toEqual('SELECT 1;');
});
