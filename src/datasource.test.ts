import { DataSource } from './datasource';
import { PinotDataQuery } from './dataquery/PinotDataQuery';
import { QueryType } from './dataquery/QueryType';
import { EditorMode } from './dataquery/EditorMode';

// getQueryDisplayText reads only its argument (no `this`), so exercise it via the prototype without
// constructing the datasource (which would need a full instanceSettings + backend mock).
const displayText = (query: Partial<PinotDataQuery>): string =>
  DataSource.prototype.getQueryDisplayText.call({} as DataSource, query as PinotDataQuery);

describe('getQueryDisplayText builder summary', () => {
  const base = { queryType: QueryType.PinotQL, editorMode: EditorMode.Builder, tableName: 't', timeColumn: 'ts' };

  it('summarizes the time-series builder shape (aggregationFunction + metricColumnV2)', () => {
    // TimeSeriesBuilder clears legacy metricColumn/groupByColumns and writes the V2 fields.
    expect(
      displayText({
        ...base,
        aggregationFunction: 'AVG',
        metricColumn: undefined,
        metricColumnV2: { name: 'latency' },
        groupByColumnsV2: [{ name: 'region' }],
        filters: [{ columnName: 'country', operator: '=', valueExprs: ["'US'", "'CA'"] }],
      })
    ).toBe("Table: t, Time: ts, Aggregation: AVG(latency), Dimensions: region, Filters: country = 'US','CA'");
  });

  it('summarizes the table builder shape (aggregations[], complex-field dimension)', () => {
    expect(
      displayText({
        ...base,
        aggregations: [
          { function: 'SUM', column: { name: 'value' } },
          { function: 'COUNT', column: {} },
        ],
        groupByColumnsV2: [{ name: 'dim', key: 'k' }],
      })
    ).toBe("Table: t, Time: ts, Aggregation: SUM(value), COUNT(*), Dimensions: dim['k'], Filters: none");
  });

  it('renders filter subqueryExpr and complex-field columnKey', () => {
    expect(
      displayText({
        ...base,
        filters: [{ columnName: 'meta', columnKey: 'region', operator: 'IN', subqueryExpr: '(SELECT x FROM y)' }],
      })
    ).toBe("Table: t, Time: ts, Aggregation: none, Dimensions: none, Filters: meta['region'] IN (SELECT x FROM y)");
  });

  it('falls back to a placeholder for an empty query', () => {
    expect(displayText({})).toBe('Empty query');
  });
});
