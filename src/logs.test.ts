import { DataQueryResponse, Field, FieldType, LogRowContextQueryDirection, toDataFrame } from '@grafana/data';
import {
  attachDerivedFieldLinks,
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
