import { replaceRateInterval, sqlUsesTimeRange } from './timeRange';

describe('sqlUsesTimeRange', () => {
  test.each([
    ['SELECT * FROM t WHERE $__timeFilter("ts")', true],
    ['SELECT * FROM t WHERE $__timeFilter', true],
    ['SELECT * FROM t WHERE $__timeFilterMillis(ts)', true],
    ['SELECT * FROM t WHERE ts >= $__timeFrom() AND ts < $__timeTo()', true],
    ['SELECT * FROM t WHERE ts >= $__timeFromMillis AND ts < $__timeToMillis', true],
    ['SELECT * FROM t WHERE ts >= $__fromTime AND ts < $__toTime', true],
    ['SELECT * FROM t WHERE ts >= $__from AND ts < $__to', true],
    ['SELECT * FROM t WHERE ts >= ${__from} AND ts < ${__to:date:seconds}', true],
    ["SELECT * FROM t WHERE service = 'api--prod' AND ts >= $__from", true],
    ["SELECT * FROM t WHERE service = 'api''--prod' AND ts >= $__from", true],
    ["SELECT * FROM t WHERE service = 'api/*' AND ts >= $__from AND other = '*/prod'", true],
    ['SELECT "api--prod" FROM t WHERE ts >= $__from', true],
    ['SELECT `api--prod` FROM t WHERE ts >= $__from', true],
    ["SELECT * FROM t WHERE event_time >= '${__from:date:iso}'", true],
    ['SELECT * FROM t WHERE ts >= now() - $__panelMillis', true],
    ['SELECT * FROM t WHERE "timestamp" >= 1775159508000 AND "timestamp" < 1775418708000', false],
    ['SELECT COUNT(*) FROM t', false],
    ['SELECT * FROM t -- WHERE $__timeFilter("ts")', false],
    ['SELECT * FROM t /* $__timeFilter("ts") */', false],
    ["SELECT * FROM t WHERE message = '$__timeFilter(ts)'", false],
    ["SELECT * FROM t WHERE message = '$__noTimeRange'", false],
    ['SELECT $__timeGroup("ts") FROM t', false],
    ['SELECT * FROM t WHERE x = $__fromage', false],
    ['SELECT COUNT(*) FROM t -- $__noTimeRange', true],
    ['SELECT COUNT(*) FROM t /* $__noTimeRange */', true],
    ['SELECT COUNT(*) FROM t -- $__noTimeRanges', false],
  ])('%s -> %s', (sql, want) => {
    expect(sqlUsesTimeRange(sql)).toBe(want);
  });
});

describe('replaceRateInterval', () => {
  test('uses 4x the panel interval', () => {
    expect(replaceRateInterval('rate(m[$__rate_interval])', { __interval_ms: { text: '', value: 30000 } })).toBe(
      'rate(m[120s])'
    );
  });
  test('supports the braced form', () => {
    expect(replaceRateInterval('rate(m[${__rate_interval}])', { __interval_ms: { text: '', value: 15000 } })).toBe(
      'rate(m[60s])'
    );
  });
  test("prefers Grafana's own value", () => {
    expect(
      replaceRateInterval('rate(m[$__rate_interval])', {
        __rate_interval: { text: '', value: '1m' },
        __interval_ms: { text: '', value: 15000 },
      })
    ).toBe('rate(m[1m])');
  });
  test('leaves the query alone without an interval', () => {
    expect(replaceRateInterval('rate(m[$__rate_interval])', {})).toBe('rate(m[$__rate_interval])');
  });
  test('does not touch $__interval', () => {
    expect(replaceRateInterval('rate(m[$__interval])', { __interval_ms: { text: '', value: 15000 } })).toBe(
      'rate(m[$__interval])'
    );
  });
});
