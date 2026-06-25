import { DataQueryResponse, Field, FieldType, LogRowContextQueryDirection, toDataFrame } from '@grafana/data';
import {
  attachDerivedFieldLinks,
  attachLogsToTracesLinks,
  logsToTracesConfig,
  LOG_ROW_CONTEXT_REF_ID,
  LOG_ROW_CONTEXT_WINDOW_MS,
  LOGS_VOLUME_REF_ID_PREFIX,
  logRowContextQuery,
  logRowContextTimeWindow,
  logsVolumeQuery,
} from './logs';
import { PinotDataQuery } from './dataquery/PinotDataQuery';
import { QueryType } from './dataquery/QueryType';
import { EditorMode } from './dataquery/EditorMode';
import { DisplayType } from './dataquery/DisplayType';

const logsQuery: PinotDataQuery = {
  refId: 'A',
  queryType: QueryType.PinotQL,
  editorMode: EditorMode.Builder,
  displayType: DisplayType.LOGS,
  tableName: 'nginxLogs',
  timeColumn: 'ts',
  logColumn: { name: 'message' },
  levelColumn: { name: 'logLevel' },
  filters: [{ columnName: 'method', operator: '=', valueExprs: ["'GET'"] }],
};

describe('logsVolumeQuery', () => {
  test('derives a logsVolume-flagged query keeping table/time/level/filters', () => {
    expect(logsVolumeQuery(logsQuery)).toEqual<PinotDataQuery>({
      ...logsQuery,
      refId: `${LOGS_VOLUME_REF_ID_PREFIX}A`,
      logsVolume: true,
    });
  });

  test('returns undefined for non-logs display types', () => {
    expect(logsVolumeQuery({ ...logsQuery, displayType: DisplayType.TIMESERIES })).toBeUndefined();
  });

  test('returns undefined when table or time column is missing', () => {
    expect(logsVolumeQuery({ ...logsQuery, tableName: undefined })).toBeUndefined();
    expect(logsVolumeQuery({ ...logsQuery, timeColumn: undefined })).toBeUndefined();
  });

  test('returns undefined for hidden queries', () => {
    expect(logsVolumeQuery({ ...logsQuery, hide: true })).toBeUndefined();
  });
});

describe('logRowContextQuery', () => {
  test('keeps table/log/time/filters, sets context direction and limit', () => {
    expect(logRowContextQuery(logsQuery, LogRowContextQueryDirection.Backward, 7)).toEqual<PinotDataQuery>({
      ...logsQuery,
      refId: LOG_ROW_CONTEXT_REF_ID,
      logContextDirection: LogRowContextQueryDirection.Backward,
      logsVolume: undefined,
      limit: 7,
    });
  });
});

describe('logRowContextTimeWindow', () => {
  const anchorMs = 1_700_000_000_000;

  test('backward looks before the anchor', () => {
    expect(logRowContextTimeWindow(anchorMs, LogRowContextQueryDirection.Backward)).toEqual({
      fromMs: anchorMs - LOG_ROW_CONTEXT_WINDOW_MS,
      toMs: anchorMs,
    });
  });

  test('forward looks after the anchor', () => {
    expect(logRowContextTimeWindow(anchorMs, LogRowContextQueryDirection.Forward)).toEqual({
      fromMs: anchorMs,
      toMs: anchorMs + LOG_ROW_CONTEXT_WINDOW_MS,
    });
  });
});

describe('attachDerivedFieldLinks', () => {
  const logsFrame = () =>
    toDataFrame({
      refId: 'A',
      fields: [
        { name: 'labels', type: FieldType.other, values: [{ traceId: 'abc' }, { traceId: 'def' }] },
        { name: 'Line', type: FieldType.string, values: ['line1', 'line2'] },
        { name: 'Time', type: FieldType.time, values: [1, 2] },
      ],
    });

  test('adds a data-link field carrying the extracted values', () => {
    const response: DataQueryResponse = { data: [logsFrame()] };
    const target: PinotDataQuery = {
      refId: 'A',
      jsonExtractors: [
        {
          source: { name: 'message' },
          path: '$.traceId',
          resultType: 'STRING',
          alias: 'traceId',
          link: 'http://trace/${__value.raw}',
        },
      ],
    };

    const derived = attachDerivedFieldLinks(response, [target]).data[0].fields.find((f: Field) => f.name === 'traceId');
    expect(derived?.config.links).toEqual([{ title: 'traceId', url: 'http://trace/${__value.raw}', targetBlank: true }]);
    expect(derived?.values).toEqual(['abc', 'def']);
  });

  test('parses string-encoded labels', () => {
    const frame = toDataFrame({
      refId: 'A',
      fields: [{ name: 'labels', type: FieldType.string, values: ['{"traceId":"xyz"}'] }],
    });
    const target: PinotDataQuery = {
      refId: 'A',
      regexpExtractors: [{ source: { name: 'message' }, pattern: '(.*)', group: 1, alias: 'traceId', link: 'http://t/$x' }],
    };

    const derived = attachDerivedFieldLinks({ data: [frame] }, [target]).data[0].fields.find((f: Field) => f.name === 'traceId');
    expect(derived?.values).toEqual(['xyz']);
  });

  test('leaves frames without linked extractors untouched', () => {
    const response: DataQueryResponse = { data: [logsFrame()] };
    const target: PinotDataQuery = {
      refId: 'A',
      jsonExtractors: [{ source: { name: 'message' }, path: '$.traceId', alias: 'traceId' }],
    };
    expect(attachDerivedFieldLinks(response, [target]).data[0].fields).toHaveLength(3);
  });
});

describe('attachLogsToTracesLinks', () => {
  const datasource = { uid: 'ds-uid', name: 'Pinot' };
  const fullConfig = { logsToTracesTable: 'otelTraces', logsToTracesTraceIdColumn: 'traceId', logsToTracesTimeColumn: 'ts' };

  const logsFrame = () =>
    toDataFrame({
      refId: 'A',
      fields: [
        { name: 'labels', type: FieldType.other, values: [{ traceId: 'abc' }, { traceId: 'def' }] },
        { name: 'Line', type: FieldType.string, values: ['line1', 'line2'] },
        { name: 'Time', type: FieldType.time, values: [1, 2] },
      ],
    });

  test('attaches an internal trace data link reading the trace id out of labels', () => {
    const response: DataQueryResponse = { data: [logsFrame()] };

    const out = attachLogsToTracesLinks(response, logsToTracesConfig(fullConfig), datasource);
    const traceField = out.data[0].fields.find((f: Field) => f.name === 'traceId');

    expect(traceField?.values).toEqual(['abc', 'def']);
    expect(traceField?.config.links).toHaveLength(1);
    const link = traceField!.config.links![0];
    expect(link.title).toBe('View trace');
    expect(link.internal?.datasourceUid).toBe('ds-uid');
    expect(link.internal?.datasourceName).toBe('Pinot');
    expect(link.internal?.query).toMatchObject({
      queryType: QueryType.PinotQL,
      editorMode: EditorMode.Builder,
      displayType: DisplayType.TRACES,
      tableName: 'otelTraces',
      timeColumn: 'ts',
      traceId: '${__value.raw}',
    });
  });

  test('parses string-encoded labels', () => {
    const frame = toDataFrame({
      refId: 'A',
      fields: [{ name: 'labels', type: FieldType.string, values: ['{"traceId":"xyz"}'] }],
    });

    const out = attachLogsToTracesLinks({ data: [frame] }, logsToTracesConfig(fullConfig), datasource);
    const traceField = out.data[0].fields.find((f: Field) => f.name === 'traceId');
    expect(traceField?.values).toEqual(['xyz']);
  });

  test('no link when the mapping is not fully configured', () => {
    const response: DataQueryResponse = { data: [logsFrame()] };

    // Each field missing in turn leaves the frame untouched (still the original 3 fields).
    expect(attachLogsToTracesLinks(response, logsToTracesConfig({}), datasource).data[0].fields).toHaveLength(3);
    expect(
      attachLogsToTracesLinks(response, logsToTracesConfig({ ...fullConfig, logsToTracesTimeColumn: undefined }), datasource)
        .data[0].fields
    ).toHaveLength(3);
  });

  test('leaves frames without a labels field untouched', () => {
    const frame = toDataFrame({
      refId: 'A',
      fields: [{ name: 'value', type: FieldType.number, values: [1, 2] }],
    });
    expect(attachLogsToTracesLinks({ data: [frame] }, logsToTracesConfig(fullConfig), datasource).data[0].fields).toHaveLength(
      1
    );
  });
});
