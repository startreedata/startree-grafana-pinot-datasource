import { searchFilterLikeValue, withSearchFilter } from './variables';
import { DataQueryRequest } from '@grafana/data';
import { PinotDataQuery } from './dataquery/PinotDataQuery';

describe('searchFilterLikeValue', () => {
  test('defaults to match-all when nothing is typed', () => {
    expect(searchFilterLikeValue('')).toBe('%');
  });

  test('appends a trailing wildcard for prefix matching', () => {
    expect(searchFilterLikeValue('check')).toBe('check%');
  });

  test('escapes single quotes to keep the SQL literal valid', () => {
    expect(searchFilterLikeValue("o'brien")).toBe("o''brien%");
  });
});

describe('withSearchFilter', () => {
  const requestWith = (scopedVars: Record<string, unknown>): DataQueryRequest<PinotDataQuery> =>
    ({ scopedVars, targets: [{ refId: 'A' }] } as unknown as DataQueryRequest<PinotDataQuery>);

  test('normalizes the typed value into a prefix LIKE pattern', () => {
    const out = withSearchFilter(requestWith({ __searchFilter: { text: 'check', value: 'check' } }));
    expect(out.scopedVars.__searchFilter).toEqual({ text: 'check', value: 'check%' });
  });

  test('defaults to match-all when Grafana provides no search filter', () => {
    const out = withSearchFilter(requestWith({}));
    expect(out.scopedVars.__searchFilter).toEqual({ text: '', value: '%' });
  });

  test('preserves other scoped vars', () => {
    const out = withSearchFilter(requestWith({ foo: { text: 'bar', value: 'bar' } }));
    expect(out.scopedVars.foo).toEqual({ text: 'bar', value: 'bar' });
  });
});
