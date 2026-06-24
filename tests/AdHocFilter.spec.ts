import { expect } from '@playwright/test';
import { addPanelInCodeMode, queryEditorTest as test, setPanelTimeWindow } from '@helpers/helpers';

/**
 * Ad-hoc filter ($__adHocFilter macro) e2e test.
 *
 * The macro is expanded server-side by the Go backend. With no active ad-hoc filters it expands to
 * `TRUE`, so a `WHERE ... AND $__adHocFilter` clause is a safe no-op. This exercises the macro
 * through the real backend (the SQL-preview path runs the same MacroEngine.ExpandMacros as a live
 * query) and confirms the resulting query parses and renders data.
 *
 * Value injection (`"col" = 'x'`, regex, escaping) is covered by the Go unit tests in
 * pkg/pinot/adhoc_filter_test.go and pkg/plugin/dataquery/sql_macros_test.go, and the TS wiring by
 * src/dataquery/PinotDataQuery.test.ts.
 *
 * Requires the `complex_website` table on the target Pinot cluster.
 */
test.describe('Ad-hoc filter macro', () => {
  test('$__adHocFilter with no filters expands to TRUE and renders data', async ({ page, datasource }) => {
    await page.goto('http://localhost:3000/dashboard/new?orgId=1');

    await addPanelInCodeMode(page, datasource.name, {
      table: 'complex_website',
      code:
        `SELECT $__timeGroup("hoursSinceEpoch", '12:HOURS') AS $__timeAlias(),\n` +
        `  SUM("views") AS $__metricAlias(), "country"\n` +
        `FROM $__table()\n` +
        `WHERE $__timeFilter("hoursSinceEpoch", '12:HOURS')\n` +
        `  AND $__adHocFilter\n` +
        `GROUP BY $__timeAlias(), "country"\n` +
        `ORDER BY $__timeAlias() DESC\n` +
        `LIMIT 100000`,
    });

    // $__adHocFilter is expanded server-side; with no active filters it becomes TRUE and the macro
    // token must be gone (proving it was recognized, not passed through to Pinot unexpanded).
    await expect(page.getByTestId('sql-preview')).toContainText('TRUE');
    await expect(page.getByTestId('sql-preview')).not.toContainText('$__adHocFilter');

    // Move the panel time window onto the fixture data (it predates the default "now"-relative
    // range) and re-run, then confirm the expanded query is valid SQL that returns data.
    await setPanelTimeWindow(page);
    await expect(page.getByText('No data')).not.toBeVisible({ timeout: 15000 });
  });
});
