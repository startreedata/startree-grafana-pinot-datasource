import { replaceAllVariableExpressionsWithSubqueries, interpolateVariables } from './PinotDataQuery';
import { setTemplateSrv, TemplateSrv } from '@grafana/runtime';
import { TypedVariableModel, QueryVariableModel, VariableOption } from '@grafana/data';
import { VariableType } from '../components/VariableQueryEditor/SelectVariableType';

function makeOptions(values: string[]): VariableOption[] {
  return values.map((v) => ({ value: v, text: v, selected: false }));
}

function makeQueryVariable(overrides: {
  name: string;
  pinotQlCode: string;
  options: string[];
  selected: string[] | string;
}): QueryVariableModel {
  const selectedValue = overrides.selected;
  return {
    name: overrides.name,
    type: 'query',
    datasource: null,
    definition: '',
    sort: 0,
    regex: '',
    refresh: 1,
    multi: true,
    includeAll: true,
    options: makeOptions(overrides.options),
    current: {
      value: selectedValue,
      text: Array.isArray(selectedValue) ? selectedValue.join(', ') : selectedValue,
      selected: true,
    },
    query: {
      variableQuery: {
        variableType: VariableType.PinotQlCode,
        pinotQlCode: overrides.pinotQlCode,
      },
    },
  } as unknown as QueryVariableModel;
}

function generateValues(count: number, prefix = 'val'): string[] {
  return Array.from({ length: count }, (_, i) => `${prefix}${i}`);
}

describe('replaceAllVariableExpressionsWithSubqueries', () => {
  describe('threshold behavior', () => {
    const subquery = 'SELECT DISTINCT playerName FROM baseballStats';
    const allOptions = generateValues(2000, 'player');

    test('below threshold — returns original SQL unchanged', () => {
      const selected = allOptions.slice(0, 999);
      const variable = makeQueryVariable({
        name: 'player',
        pinotQlCode: subquery,
        options: allOptions,
        selected,
      });

      const sql = "SELECT * FROM baseballStats WHERE playerName IN (${player:singlequote})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [variable]);
      expect(result).toBe(sql);
    });

    test('at threshold (1000) — returns original SQL unchanged', () => {
      const selected = allOptions.slice(0, 1000);
      const variable = makeQueryVariable({
        name: 'player',
        pinotQlCode: subquery,
        options: allOptions,
        selected,
      });

      const sql = "SELECT * FROM baseballStats WHERE playerName IN (${player:singlequote})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [variable]);
      expect(result).toBe(sql);
    });

    test('above threshold (1001) — replaces with subquery', () => {
      const selected = allOptions.slice(0, 1001);
      const variable = makeQueryVariable({
        name: 'player',
        pinotQlCode: subquery,
        options: allOptions,
        selected,
      });

      const sql = "SELECT * FROM baseballStats WHERE playerName IN (${player:singlequote})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [variable]);
      expect(result).toContain('SELECT DISTINCT playerName FROM baseballStats');
      expect(result).not.toContain('${player');
    });
  });

  describe('excluded-set algorithm', () => {
    const subquery = 'SELECT DISTINCT playerName FROM baseballStats';
    const allOptions = generateValues(2000, 'player');

    test('all selected — pure subquery', () => {
      const variable = makeQueryVariable({
        name: 'player',
        pinotQlCode: subquery,
        options: allOptions,
        selected: allOptions,
      });

      const sql = "SELECT * FROM baseballStats WHERE playerName IN (${player})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [variable]);
      expect(result).toBe(`SELECT * FROM baseballStats WHERE playerName IN (${subquery})`);
    });

    test('$__all selected — pure subquery', () => {
      const variable = makeQueryVariable({
        name: 'player',
        pinotQlCode: subquery,
        options: allOptions,
        selected: '$__all',
      });

      const sql = "SELECT * FROM baseballStats WHERE playerName IN (${player})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [variable]);
      expect(result).toBe(`SELECT * FROM baseballStats WHERE playerName IN (${subquery})`);
    });

    test('all-but-3 selected — subquery with NOT IN exclusions', () => {
      const excluded = ['player0', 'player1', 'player2'];
      const selected = allOptions.filter((v) => !excluded.includes(v));
      const variable = makeQueryVariable({
        name: 'player',
        pinotQlCode: subquery,
        options: allOptions,
        selected,
      });

      const sql = "SELECT * FROM baseballStats WHERE playerName IN (${player})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [variable]);
      expect(result).toBe(
        `SELECT * FROM baseballStats WHERE playerName IN (SELECT "playerName" FROM (${subquery}) WHERE "playerName" NOT IN ('player0', 'player1', 'player2'))`
      );
    });

    test('excluded > 1000 — falls back to pure subquery', () => {
      // Select 1001 out of 2100 (excluded = 1099 > 1000)
      const bigOptions = generateValues(2100, 'player');
      const selected = bigOptions.slice(0, 1001);
      const variable = makeQueryVariable({
        name: 'player',
        pinotQlCode: subquery,
        options: bigOptions,
        selected,
      });

      const sql = "SELECT * FROM baseballStats WHERE playerName IN (${player})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [variable]);
      expect(result).toBe(`SELECT * FROM baseballStats WHERE playerName IN (${subquery})`);
    });

    test('subquery with existing WHERE clause — wraps in derived table', () => {
      const subqueryWithWhere = 'SELECT DISTINCT playerName FROM baseballStats WHERE league = \'MLB\'';
      const excluded = ['player5'];
      const selected = allOptions.filter((v) => !excluded.includes(v));
      const variable = makeQueryVariable({
        name: 'player',
        pinotQlCode: subqueryWithWhere,
        options: allOptions,
        selected,
      });

      const sql = "SELECT * FROM baseballStats WHERE playerName IN (${player})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [variable]);
      expect(result).toBe(
        `SELECT * FROM baseballStats WHERE playerName IN (SELECT "playerName" FROM (${subqueryWithWhere}) WHERE "playerName" NOT IN ('player5'))`
      );
    });
  });

  describe('variable pattern matching', () => {
    const subquery = 'SELECT DISTINCT col FROM tbl';
    const allOptions = generateValues(2000);

    function makeVar(name: string) {
      return makeQueryVariable({
        name,
        pinotQlCode: subquery,
        options: allOptions,
        selected: allOptions,
      });
    }

    test('matches ${var:singlequote} format', () => {
      const result = replaceAllVariableExpressionsWithSubqueries("WHERE x IN (${myVar:singlequote})", [makeVar('myVar')]);
      expect(result).toBe(`WHERE x IN (${subquery})`);
    });

    test('matches ${var:csv} format', () => {
      const result = replaceAllVariableExpressionsWithSubqueries("WHERE x IN (${myVar:csv})", [makeVar('myVar')]);
      expect(result).toBe(`WHERE x IN (${subquery})`);
    });

    test('matches ${var:pipe} format', () => {
      const result = replaceAllVariableExpressionsWithSubqueries("WHERE x IN (${myVar:pipe})", [makeVar('myVar')]);
      expect(result).toBe(`WHERE x IN (${subquery})`);
    });

    test('matches ${var} format', () => {
      const result = replaceAllVariableExpressionsWithSubqueries("WHERE x IN (${myVar})", [makeVar('myVar')]);
      expect(result).toBe(`WHERE x IN (${subquery})`);
    });

    test('matches $var format', () => {
      const result = replaceAllVariableExpressionsWithSubqueries("WHERE x IN ($myVar)", [makeVar('myVar')]);
      expect(result).toBe(`WHERE x IN (${subquery})`);
    });

    test('does not match $var when followed by word char', () => {
      const result = replaceAllVariableExpressionsWithSubqueries("WHERE x IN ($myVarExtra)", [makeVar('myVar')]);
      expect(result).toBe("WHERE x IN ($myVarExtra)");
    });
  });

  describe('edge cases', () => {
    test('non-query variable is skipped', () => {
      const variable = {
        name: 'interval',
        type: 'interval',
        options: [],
        current: { value: '1m', text: '1m', selected: true },
        query: '',
        auto: false,
        auto_min: '',
        auto_count: 0,
      } as unknown as TypedVariableModel;

      const sql = "WHERE x IN (${interval})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [variable]);
      expect(result).toBe(sql);
    });

    test('variable without pinotQlCode is skipped', () => {
      const variable = makeQueryVariable({
        name: 'player',
        pinotQlCode: '',
        options: generateValues(2000),
        selected: generateValues(2000),
      });
      // Override to have no pinotQlCode
      (variable.query as any).variableQuery.pinotQlCode = undefined;

      const sql = "WHERE x IN (${player})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [variable]);
      expect(result).toBe(sql);
    });

    test('empty options array is skipped', () => {
      const variable = makeQueryVariable({
        name: 'player',
        pinotQlCode: 'SELECT DISTINCT col FROM tbl',
        options: [],
        selected: [],
      });

      const sql = "WHERE x IN (${player})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [variable]);
      expect(result).toBe(sql);
    });

    test('unparseable subquery column with excluded values — skips optimization (returns null)', () => {
      // Some-but-not-all selected (~50% excluded) where the subquery's first column can't be parsed
      // (e.g. `SELECT *` or aliased columns). Returning the raw subquery here would silently match
      // ALL values and drop the user's exclusions, so we now skip the optimization entirely and
      // leave the variable reference for templateSrv.replace() to handle as a literal IN clause.
      const weirdSubquery = 'SELECT * FROM tbl';
      const allOptions = generateValues(2000);
      const excluded = ['val0'];
      const selected = allOptions.filter((v) => !excluded.includes(v));
      const variable = makeQueryVariable({
        name: 'x',
        pinotQlCode: weirdSubquery,
        options: allOptions,
        selected,
      });

      const sql = "WHERE col IN (${x})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [variable]);
      expect(result).toBe("WHERE col IN (${x})");
    });

    test('unparseable subquery column with all selected — returns raw subquery', () => {
      // When every value is selected, no NOT IN filter is needed, so the column-parse failure
      // doesn't matter — we just inject the raw subquery.
      const weirdSubquery = 'SELECT * FROM tbl';
      const allOptions = generateValues(2000);
      const variable = makeQueryVariable({
        name: 'x',
        pinotQlCode: weirdSubquery,
        options: allOptions,
        selected: allOptions,
      });

      const sql = "WHERE col IN (${x})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [variable]);
      expect(result).toBe(`WHERE col IN (${weirdSubquery})`);
    });

    test('multiple variables in one query', () => {
      const allA = generateValues(2000, 'a');
      const allB = generateValues(2000, 'b');
      const varA = makeQueryVariable({
        name: 'varA',
        pinotQlCode: 'SELECT DISTINCT colA FROM tblA',
        options: allA,
        selected: allA,
      });
      const varB = makeQueryVariable({
        name: 'varB',
        pinotQlCode: 'SELECT DISTINCT colB FROM tblB',
        options: allB,
        selected: allB,
      });

      const sql = "WHERE a IN (${varA}) AND b IN (${varB})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [varA, varB]);
      expect(result).toBe(
        "WHERE a IN (SELECT DISTINCT colA FROM tblA) AND b IN (SELECT DISTINCT colB FROM tblB)"
      );
    });

    test('SQL injection in excluded values is escaped', () => {
      const subquery = 'SELECT DISTINCT name FROM tbl';
      const allOptions = [...generateValues(2000), "O'Brien"];
      const selected = allOptions.filter((v) => v !== "O'Brien");
      const variable = makeQueryVariable({
        name: 'x',
        pinotQlCode: subquery,
        options: allOptions,
        selected,
      });

      const sql = "WHERE name IN (${x})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [variable]);
      expect(result).toContain("'O''Brien'");
    });

    test('variable backing SQL with trailing semicolon — semicolon stripped before injection', () => {
      // Variables created via the SQL Query tab often end with `;`. If we inject the SQL as a
      // subquery without stripping the `;`, Pinot rejects `IN (SELECT ... LIMIT N;)` as a syntax
      // error. Verify getVariableSubquery strips the trailing `;` (and surrounding whitespace).
      const subquery = 'SELECT DISTINCT entity FROM highCardinality LIMIT 4000';
      const allOptions = generateValues(2000, 'entity');
      const variable = makeQueryVariable({
        name: 'entity',
        pinotQlCode: `${subquery};  \n`,
        options: allOptions,
        selected: allOptions,
      });

      const sql = "WHERE entity IN (${entity})";
      const result = replaceAllVariableExpressionsWithSubqueries(sql, [variable]);
      expect(result).toBe(`WHERE entity IN (${subquery})`);
      expect(result).not.toContain(';)');
    });

    test('variable reference outside IN(...) context is NOT replaced (code-mode contextual safety)', () => {
      // Pre-fix, every occurrence of ${var} got rewritten to the subquery, so
      // `WHERE col = ${var}` became `WHERE col = SELECT ...` which is invalid SQL. The fix
      // restricts the replacement to references inside `IN (...)` / `NOT IN (...)` only.
      const subquery = 'SELECT DISTINCT entity FROM highCardinality';
      const allOptions = generateValues(2000, 'entity');
      const variable = makeQueryVariable({
        name: 'entity',
        pinotQlCode: subquery,
        options: allOptions,
        selected: allOptions,
      });

      // Equality position — must NOT be rewritten.
      const equalitySql = "WHERE entity = ${entity}";
      expect(replaceAllVariableExpressionsWithSubqueries(equalitySql, [variable])).toBe(equalitySql);

      // SELECT projection — must NOT be rewritten.
      const selectSql = "SELECT ${entity} AS first_entity FROM tbl LIMIT 1";
      expect(replaceAllVariableExpressionsWithSubqueries(selectSql, [variable])).toBe(selectSql);

      // IN (...) — must be rewritten.
      const inSql = "WHERE entity IN (${entity})";
      expect(replaceAllVariableExpressionsWithSubqueries(inSql, [variable])).toBe(
        `WHERE entity IN (${subquery})`
      );

      // NOT IN (...) — must also be rewritten.
      const notInSql = "WHERE entity NOT IN (${entity})";
      expect(replaceAllVariableExpressionsWithSubqueries(notInSql, [variable])).toBe(
        `WHERE entity NOT IN (${subquery})`
      );
    });
  });
});

describe('interpolateVariables — Builder mode subquery optimization', () => {
  afterEach(() => {
    setTemplateSrv(undefined as unknown as TemplateSrv);
  });

  test('filter with template variable exceeding threshold gets subqueryExpr', () => {
    const allOptions = generateValues(2000, 'player');
    const variable = makeQueryVariable({
      name: 'player',
      pinotQlCode: 'SELECT DISTINCT playerName FROM baseballStats',
      options: allOptions,
      selected: allOptions,
    });

    setTemplateSrv({
      containsTemplate: () => true,
      getVariables: () => [variable] as unknown as TypedVariableModel[],
      updateTimeRange: () => {},
      replace: (target?: string) => target || '',
    });

    const query = interpolateVariables({
      refId: 'A',
      filters: [
        {
          columnName: 'playerName',
          operator: '=',
          valueExprs: ['${player}'],
        },
      ],
    });

    expect(query.filters?.[0].subqueryExpr).toBe(
      'SELECT DISTINCT playerName FROM baseballStats'
    );
    expect(query.filters?.[0].operator).toBe('in');
    expect(query.filters?.[0].valueExprs).toBeUndefined();
  });

  test('filter with template variable below threshold — multi-value — expands as quoted literals with IN', () => {
    // This is the "category has 6 values" scenario from the bug report.
    // When a query variable has multiple selected values but fewer than 1000,
    // the filter should expand to proper SQL literals with operator changed to IN.
    // Without this, templateSrv.replace("$category") returns Grafana's raw format
    // {cache,api,null,web,db,queue} which is invalid SQL.
    const categories = ['cache', 'api', 'null', 'web', 'db', 'queue'];
    const variable = makeQueryVariable({
      name: 'category',
      pinotQlCode: 'SELECT DISTINCT category FROM highCardinality',
      options: categories,
      selected: categories, // all 6 selected — below 1000 threshold
    });

    setTemplateSrv({
      containsTemplate: () => true,
      getVariables: () => [variable] as unknown as TypedVariableModel[],
      updateTimeRange: () => {},
      replace: (target?: string) => target || '',
    });

    const query = interpolateVariables({
      refId: 'A',
      filters: [
        {
          columnName: 'category',
          operator: '=',
          valueExprs: ['$category'],
        },
      ],
    });

    // Should expand to quoted literals and switch to IN operator
    expect(query.filters?.[0].subqueryExpr).toBeUndefined();
    expect(query.filters?.[0].operator).toBe('in');
    expect(query.filters?.[0].valueExprs).toEqual(
      ["'cache'", "'api'", "'null'", "'web'", "'db'", "'queue'"]
    );
  });

  test('filter with template variable below threshold — single value — uses normal replace path', () => {
    // When only 1 value is selected, let templateSrv.replace handle it normally
    const categories = ['cache', 'api', 'null', 'web', 'db', 'queue'];
    const variable = makeQueryVariable({
      name: 'category',
      pinotQlCode: 'SELECT DISTINCT category FROM highCardinality',
      options: categories,
      selected: 'cache', // single value
    });

    setTemplateSrv({
      containsTemplate: () => true,
      getVariables: () => [variable] as unknown as TypedVariableModel[],
      updateTimeRange: () => {},
      replace: (target?: string) => target === '$category' ? "'cache'" : target || '',
    });

    const query = interpolateVariables({
      refId: 'A',
      filters: [
        {
          columnName: 'category',
          operator: '=',
          valueExprs: ['$category'],
        },
      ],
    });

    // Single value — operator and path unchanged
    expect(query.filters?.[0].subqueryExpr).toBeUndefined();
    expect(query.filters?.[0].operator).toBe('=');
    expect(query.filters?.[0].valueExprs).toEqual(["'cache'"]);
  });

  test('multiple filters — high-cardinality gets subquery, low-cardinality gets quoted literals', () => {
    // This is exactly the screenshot scenario: entity (1501 values) + category (6 values)
    const entityOptions = generateValues(1501, 'entity');
    const categories = ['cache', 'api', 'null', 'web', 'db', 'queue'];

    const entityVar = makeQueryVariable({
      name: 'entity',
      pinotQlCode: 'SELECT DISTINCT entity FROM highCardinality limit 4000',
      options: entityOptions,
      selected: entityOptions, // all 1501 — above threshold
    });
    const categoryVar = makeQueryVariable({
      name: 'category',
      pinotQlCode: 'SELECT DISTINCT category FROM highCardinality',
      options: categories,
      selected: categories, // all 6 — below threshold
    });

    setTemplateSrv({
      containsTemplate: () => true,
      getVariables: () => [entityVar, categoryVar] as unknown as TypedVariableModel[],
      updateTimeRange: () => {},
      replace: (target?: string) => target || '',
    });

    const query = interpolateVariables({
      refId: 'A',
      filters: [
        {
          columnName: 'entity',
          operator: '=',
          valueExprs: ['$entity'],
        },
        {
          columnName: 'category',
          operator: '=',
          valueExprs: ['$category'],
        },
      ],
    });

    // entity filter: subquery path
    expect(query.filters?.[0].operator).toBe('in');
    expect(query.filters?.[0].subqueryExpr).toBe(
      'SELECT DISTINCT entity FROM highCardinality limit 4000'
    );
    expect(query.filters?.[0].valueExprs).toBeUndefined();

    // category filter: literal expansion with IN
    expect(query.filters?.[1].operator).toBe('in');
    expect(query.filters?.[1].subqueryExpr).toBeUndefined();
    expect(query.filters?.[1].valueExprs).toEqual(
      ["'cache'", "'api'", "'null'", "'web'", "'db'", "'queue'"]
    );

    // useMultistageEngine injected because subquery fired for entity (canonical Pinot option name)
    expect(query.queryOptions).toEqual(
      expect.arrayContaining([{ name: 'useMultistageEngine', value: 'true' }])
    );
  });

  test('filter with != operator and multi-value below threshold gets NOT IN with literals', () => {
    const categories = ['cache', 'api', 'web'];
    const variable = makeQueryVariable({
      name: 'category',
      pinotQlCode: 'SELECT DISTINCT category FROM highCardinality',
      options: categories,
      selected: categories,
    });

    setTemplateSrv({
      containsTemplate: () => true,
      getVariables: () => [variable] as unknown as TypedVariableModel[],
      updateTimeRange: () => {},
      replace: (target?: string) => target || '',
    });

    const query = interpolateVariables({
      refId: 'A',
      filters: [
        {
          columnName: 'category',
          operator: '!=',
          valueExprs: ['$category'],
        },
      ],
    });

    expect(query.filters?.[0].operator).toBe('not in');
    expect(query.filters?.[0].subqueryExpr).toBeUndefined();
    expect(query.filters?.[0].valueExprs).toEqual(["'cache'", "'api'", "'web'"]);
  });

  test('filter with != operator gets mapped to not in', () => {
    const allOptions = generateValues(2000, 'player');
    const variable = makeQueryVariable({
      name: 'player',
      pinotQlCode: 'SELECT DISTINCT playerName FROM baseballStats',
      options: allOptions,
      selected: allOptions,
    });

    setTemplateSrv({
      containsTemplate: () => true,
      getVariables: () => [variable] as unknown as TypedVariableModel[],
      updateTimeRange: () => {},
      replace: (target?: string) => target || '',
    });

    const query = interpolateVariables({
      refId: 'A',
      filters: [
        {
          columnName: 'playerName',
          operator: '!=',
          valueExprs: ['${player}'],
        },
      ],
    });

    expect(query.filters?.[0].operator).toBe('not in');
    expect(query.filters?.[0].subqueryExpr).toBeDefined();
  });
});

describe('interpolateVariables — Code mode subquery optimization', () => {
  afterEach(() => {
    setTemplateSrv(undefined as unknown as TemplateSrv);
  });

  test('pinotQlCode with variable above threshold gets subquery replacement', () => {
    const allOptions = generateValues(2000, 'player');
    const variable = makeQueryVariable({
      name: 'player',
      pinotQlCode: 'SELECT DISTINCT playerName FROM baseballStats',
      options: allOptions,
      selected: allOptions,
    });

    setTemplateSrv({
      containsTemplate: () => true,
      getVariables: () => [variable] as unknown as TypedVariableModel[],
      updateTimeRange: () => {},
      replace: (target?: string) => target || '',
    });

    const query = interpolateVariables({
      refId: 'A',
      pinotQlCode: "SELECT * FROM baseballStats WHERE playerName IN (${player:singlequote})",
    });

    expect(query.pinotQlCode).toBe(
      "SELECT * FROM baseballStats WHERE playerName IN (SELECT DISTINCT playerName FROM baseballStats);\n\nSET useMultistageEngine=true;"
    );
  });

  test('pinotQlCode with variable below threshold passes through for normal expansion', () => {
    const allOptions = generateValues(500, 'player');
    const variable = makeQueryVariable({
      name: 'player',
      pinotQlCode: 'SELECT DISTINCT playerName FROM baseballStats',
      options: allOptions,
      selected: allOptions.slice(0, 100),
    });

    const expandedValue = allOptions.slice(0, 100).map((v) => `'${v}'`).join(', ');
    setTemplateSrv({
      containsTemplate: () => true,
      getVariables: () => [variable] as unknown as TypedVariableModel[],
      updateTimeRange: () => {},
      replace: (target?: string) => {
        if (target?.includes('${player')) {
          return `SELECT * FROM baseballStats WHERE playerName IN (${expandedValue})`;
        }
        return target || '';
      },
    });

    const query = interpolateVariables({
      refId: 'A',
      pinotQlCode: "SELECT * FROM baseballStats WHERE playerName IN (${player:singlequote})",
    });

    // The variable reference should still be in the string (subquery replacement didn't touch it)
    // then templateSrv.replace handles the normal expansion
    expect(query.pinotQlCode).toContain('player');
  });

  test('pinotQlCode with existing SET useMultiStageEngine=true is not duplicated', () => {
    const allOptions = generateValues(2000, 'player');
    const variable = makeQueryVariable({
      name: 'player',
      pinotQlCode: 'SELECT DISTINCT playerName FROM baseballStats',
      options: allOptions,
      selected: allOptions,
    });

    setTemplateSrv({
      containsTemplate: () => true,
      getVariables: () => [variable] as unknown as TypedVariableModel[],
      updateTimeRange: () => {},
      replace: (target?: string) => target || '',
    });

    const query = interpolateVariables({
      refId: 'A',
      pinotQlCode:
        "SELECT * FROM baseballStats WHERE playerName IN (${player:singlequote});\nSET useMultiStageEngine=true;",
    });

    const matches = query.pinotQlCode?.match(/SET\s+useMultiStageEngine/gi) ?? [];
    expect(matches.length).toBe(1);
  });

  test('pinotQlCode with explicit SET useMultiStageEngine=false is preserved (not overridden)', () => {
    const allOptions = generateValues(2000, 'player');
    const variable = makeQueryVariable({
      name: 'player',
      pinotQlCode: 'SELECT DISTINCT playerName FROM baseballStats',
      options: allOptions,
      selected: allOptions,
    });

    setTemplateSrv({
      containsTemplate: () => true,
      getVariables: () => [variable] as unknown as TypedVariableModel[],
      updateTimeRange: () => {},
      replace: (target?: string) => target || '',
    });

    const query = interpolateVariables({
      refId: 'A',
      pinotQlCode:
        "SELECT * FROM baseballStats WHERE playerName IN (${player:singlequote});\nSET useMultiStageEngine=false;",
    });

    expect(query.pinotQlCode).toContain('SET useMultiStageEngine=false;');
    expect(query.pinotQlCode).not.toContain('SET useMultiStageEngine=true;');
  });

  test('SET useMultiStageEngine match is case-insensitive', () => {
    const allOptions = generateValues(2000, 'player');
    const variable = makeQueryVariable({
      name: 'player',
      pinotQlCode: 'SELECT DISTINCT playerName FROM baseballStats',
      options: allOptions,
      selected: allOptions,
    });

    setTemplateSrv({
      containsTemplate: () => true,
      getVariables: () => [variable] as unknown as TypedVariableModel[],
      updateTimeRange: () => {},
      replace: (target?: string) => target || '',
    });

    const query = interpolateVariables({
      refId: 'A',
      pinotQlCode:
        "SELECT * FROM baseballStats WHERE playerName IN (${player:singlequote});\nset usemultistageengine = true;",
    });

    const matches = query.pinotQlCode?.match(/SET\s+useMultiStageEngine/gi) ?? [];
    expect(matches.length).toBe(1);
  });
});
