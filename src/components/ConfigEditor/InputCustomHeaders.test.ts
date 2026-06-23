import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { readHeaderRows, withHeaderRows } from './InputCustomHeaders';
import { PinotConnectionConfig, PinotSecureConfig } from '../../config/PinotConnectionConfig';

type Options = DataSourcePluginOptionsEditorProps<PinotConnectionConfig, PinotSecureConfig>['options'];

function makeOptions(over: Partial<{ jsonData: object; secureJsonData: object; secureJsonFields: object }> = {}): Options {
  return {
    jsonData: { queryOptions: [], ...(over.jsonData || {}) },
    secureJsonData: over.secureJsonData || {},
    secureJsonFields: over.secureJsonFields || {},
  } as unknown as Options;
}

const j = (o: Options) => o.jsonData as unknown as Record<string, unknown>;
const s = (o: Options) => (o.secureJsonData || {}) as Record<string, unknown>;
const f = (o: Options) => (o.secureJsonFields || {}) as Record<string, unknown>;

test('adding a header writes the standard contiguous keys', () => {
  const out = withHeaderRows(makeOptions(), [{ name: 'X-A', value: 'secret', configured: false }]);
  expect(j(out).httpHeaderName1).toBe('X-A');
  expect(s(out).httpHeaderValue1).toBe('secret');
});

test('readHeaderRows round-trips what withHeaderRows wrote', () => {
  const out = withHeaderRows(makeOptions(), [{ name: 'X-A', value: 'secret', configured: false }]);
  expect(readHeaderRows(out)).toEqual([{ name: 'X-A', value: 'secret', configured: false }]);
});

test('removing the first of two reindexes the rest contiguously', () => {
  const two = withHeaderRows(makeOptions(), [
    { name: 'X-A', value: 'a', configured: false },
    { name: 'X-B', value: 'b', configured: false },
  ]);
  const afterRemove = withHeaderRows(two, readHeaderRows(two).filter((_, i) => i !== 0));
  expect(j(afterRemove).httpHeaderName1).toBe('X-B');
  expect(j(afterRemove).httpHeaderName2).toBeUndefined();
  expect(s(afterRemove).httpHeaderValue1).toBe('b');
  expect(s(afterRemove).httpHeaderValue2).toBeUndefined();
});

test('a configured (saved) value is referenced by index, not re-written as plaintext', () => {
  const opts = makeOptions({ jsonData: { httpHeaderName1: 'X-A' }, secureJsonFields: { httpHeaderValue1: true } });
  expect(readHeaderRows(opts)).toEqual([{ name: 'X-A', value: '', configured: true }]);

  const rewritten = withHeaderRows(opts, readHeaderRows(opts));
  expect(f(rewritten).httpHeaderValue1).toBe(true);
  expect(s(rewritten).httpHeaderValue1).toBeUndefined();
});
