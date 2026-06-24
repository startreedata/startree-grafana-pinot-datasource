import { PINOT_MACROS, formatPinotSql, lintPinotSql, macroInsertText } from './pinotLanguage';
import { DefaultQuerySql } from './CodeQuery';

describe('formatPinotSql', () => {
  it('upper-cases keywords and puts each clause on its own line', () => {
    const input = 'select a, b from t where a > 1 group by a order by a desc limit 10';
    expect(formatPinotSql(input)).toBe(
      ['SELECT a, b', 'FROM t', 'WHERE a > 1', 'GROUP BY a', 'ORDER BY a DESC', 'LIMIT 10'].join('\n')
    );
  });

  it('collapses excess whitespace', () => {
    expect(formatPinotSql('SELECT    a\n\n  FROM   t')).toBe('SELECT a\nFROM t');
  });

  it('leaves macros and quoted literals untouched', () => {
    const result = formatPinotSql('select $__timeGroup("ts") from $__table() where x = \'from where select\'');
    expect(result).toContain('$__timeGroup("ts")');
    expect(result).toContain('$__table()');
    // The keywords inside the string literal must not be reformatted.
    expect(result).toContain("'from where select'");
  });

  it('round-trips the already-formatted default query', () => {
    expect(formatPinotSql(DefaultQuerySql)).toBe(DefaultQuerySql);
  });
});

describe('macroInsertText', () => {
  it('renders a no-arg macro as an empty call', () => {
    expect(macroInsertText({ text: '$__table', args: [], description: '' })).toBe('\\$__table() ');
  });

  it('renders required args as numbered snippet placeholders', () => {
    expect(macroInsertText({ text: '$__timeGroup', args: ['"column"'], description: '' })).toBe(
      '\\$__timeGroup(${0:"column"}) '
    );
  });
});

describe('lintPinotSql', () => {
  it('returns no diagnostics for a valid query', () => {
    expect(lintPinotSql(DefaultQuerySql)).toEqual([]);
  });

  it('flags an unclosed parenthesis', () => {
    const diagnostics = lintPinotSql('SELECT count(* FROM t');
    expect(diagnostics).toHaveLength(1);
    expect(diagnostics[0].message).toMatch(/parenthesis/i);
  });

  it('flags an unmatched closing parenthesis', () => {
    const diagnostics = lintPinotSql('SELECT a) FROM t');
    expect(diagnostics.map((d) => d.message)).toContain('Unmatched closing parenthesis.');
  });

  it('flags an unterminated string literal', () => {
    const diagnostics = lintPinotSql("SELECT a FROM t WHERE x = 'oops");
    expect(diagnostics.some((d) => /Unterminated string/.test(d.message))).toBe(true);
  });

  it('flags an unknown macro but accepts known ones', () => {
    expect(lintPinotSql('SELECT $__table() FROM t')).toEqual([]);
    const diagnostics = lintPinotSql('SELECT $__bogus() FROM t');
    expect(diagnostics).toHaveLength(1);
    expect(diagnostics[0].message).toBe('Unknown macro "$__bogus".');
  });

  it('ignores macro-like text inside string literals', () => {
    expect(lintPinotSql("SELECT a FROM t WHERE note = '$__notamacro'")).toEqual([]);
  });
});

describe('PINOT_MACROS', () => {
  it('covers every server-side macro from sql_macros.go', () => {
    const names = PINOT_MACROS.map((m) => m.text).sort();
    expect(names).toEqual(
      [
        '$__granularityMillis',
        '$__metricAlias',
        '$__panelMillis',
        '$__table',
        '$__timeAlias',
        '$__timeFilter',
        '$__timeFilterMillis',
        '$__timeFrom',
        '$__timeFromMillis',
        '$__timeGroup',
        '$__timeTo',
        '$__timeToMillis',
      ].sort()
    );
  });
});
