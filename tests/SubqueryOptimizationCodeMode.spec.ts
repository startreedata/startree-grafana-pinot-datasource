import { expect, Page } from '@playwright/test';
import {
  addPanelInCodeMode,
  addPinotQueryVariable,
  queryEditorTest as test,
} from '@helpers/helpers';

/**
 * Subquery optimization e2e tests — Code (raw SQL) mode.
 *
 * Exercises the Code-mode interpolation path added in PR #184:
 *   - High-cardinality (>1000) `IN (${var:singlequote})` → subquery substitution + auto-appended SET useMultistageEngine=true;
 *   - Low-cardinality `IN (${var:singlequote})` → IN literals expansion (no MSE)
 *   - Non-IN context (`WHERE col = $var`) → variable left for templateSrv (no rewrite)
 *   - User-supplied `SET useMultistageEngine=false;` is preserved (not duplicated/overridden)
 *   - `$__table()` macro inside variable backing SQL is expanded when injected as subquery
 *
 * Requires the `highCardinality` table (1500 distinct entities) and `complex_website` (~250
 * countries) on the target Pinot cluster.
 */
test.describe('Code mode subquery optimization', () => {
  /**
   * Scenario 6: `IN (${var:singlequote})` with var >1000 → subquery + SET MSE auto-appended.
   */
  test('6. Code mode IN with high-card var → subquery + SET MSE auto-appended', async ({ page, datasource }) => {
    await openNewDashboard(page);

    await addPinotQueryVariable(page, datasource.name, {
      name: 'entity',
      sql: 'SELECT DISTINCT entity FROM highCardinality LIMIT 4000',
      multi: true,
      includeAll: true,
    });

    await selectAllInVariable(page, 'entity');

    await addPanelInCodeMode(page, datasource.name, {
      table: 'highCardinality',
      code:
        `SELECT $__timeGroup("ts") AS $__timeAlias(),\n` +
        `  SUM("value") AS $__metricAlias()\n` +
        `FROM $__table()\n` +
        `WHERE $__timeFilter("ts")\n` +
        `  AND entity IN (\${entity:singlequote})\n` +
        `GROUP BY $__timeAlias()\n` +
        `ORDER BY $__timeAlias() DESC\n` +
        `LIMIT 100000`,
    });

    // Subquery should be injected in place of the variable reference, MSE auto-appended.
    await expect(page.getByTestId('sql-preview')).toContainText(
      `entity IN (SELECT DISTINCT entity FROM highCardinality LIMIT 4000)`
    );
    await expect(page.getByTestId('sql-preview')).toContainText(`SET useMultistageEngine=true;`);
  });

  /**
   * Scenario 7: `IN (${var:singlequote})` with var ≤ threshold → IN literals expansion, no MSE.
   */
  test('7. Code mode IN with low-card var → IN literals expansion (no MSE)', async ({ page, datasource }) => {
    await openNewDashboard(page);

    await addPinotQueryVariable(page, datasource.name, {
      name: 'country',
      sql: 'SELECT DISTINCT country FROM complex_website',
      multi: true,
      includeAll: true,
    });

    await selectAllInVariable(page, 'country');

    await addPanelInCodeMode(page, datasource.name, {
      table: 'complex_website',
      code:
        `SELECT $__timeGroup("hoursSinceEpoch") AS $__timeAlias(),\n` +
        `  SUM("views") AS $__metricAlias()\n` +
        `FROM $__table()\n` +
        `WHERE $__timeFilter("hoursSinceEpoch")\n` +
        `  AND country IN (\${country:singlequote})\n` +
        `GROUP BY $__timeAlias()\n` +
        `ORDER BY $__timeAlias() DESC\n` +
        `LIMIT 100000`,
    });

    // Country has ~250 distinct values — below threshold, expanded as quoted literals.
    await expect(page.getByTestId('sql-preview')).toContainText(`country IN (`);
    await expect(page.getByTestId('sql-preview')).not.toContainText(`SELECT DISTINCT country FROM`);
    await expect(page.getByTestId('sql-preview')).not.toContainText(`SET useMultistageEngine=true;`);
  });

  /**
   * Scenario 8: `WHERE col = $var` (non-IN context) — variable is NOT replaced with subquery.
   * Pre-fix, the regex matched any occurrence and rewrote `WHERE col = $var` to `WHERE col = SELECT ...`
   * which was invalid SQL. Fix restricts replacement to IN/NOT IN contexts only.
   */
  test('8. Code mode non-IN context → variable left for templateSrv (no subquery rewrite)', async ({ page, datasource }) => {
    await openNewDashboard(page);

    await addPinotQueryVariable(page, datasource.name, {
      name: 'entity',
      sql: 'SELECT DISTINCT entity FROM highCardinality LIMIT 4000',
      multi: true,
      includeAll: true,
    });

    await selectAllInVariable(page, 'entity');

    await addPanelInCodeMode(page, datasource.name, {
      table: 'highCardinality',
      code:
        `SELECT "ts", "value"\n` +
        `FROM $__table()\n` +
        `WHERE $__timeFilter("ts")\n` +
        `  AND entity = $entity\n` +
        `LIMIT 100`,
    });

    // The variable reference must NOT be substituted with the subquery — it should pass through
    // to templateSrv (which produces Grafana's default multi-value format, valid or not).
    // The optimization fired only on IN/NOT IN positions per our contextual regex fix.
    await expect(page.getByTestId('sql-preview')).not.toContainText(
      `entity = (SELECT DISTINCT entity FROM`
    );
    await expect(page.getByTestId('sql-preview')).not.toContainText(`SET useMultistageEngine=true;`);
  });

  /**
   * Scenario 9: User-supplied `SET useMultistageEngine=false;` is preserved (not duplicated/overridden).
   */
  test('9. Code mode user-set SET useMultistageEngine=false is preserved', async ({ page, datasource }) => {
    await openNewDashboard(page);

    await addPinotQueryVariable(page, datasource.name, {
      name: 'entity',
      sql: 'SELECT DISTINCT entity FROM highCardinality LIMIT 4000',
      multi: true,
      includeAll: true,
    });

    await selectAllInVariable(page, 'entity');

    await addPanelInCodeMode(page, datasource.name, {
      table: 'highCardinality',
      code:
        `SELECT $__timeGroup("ts") AS $__timeAlias(),\n` +
        `  SUM("value") AS $__metricAlias()\n` +
        `FROM $__table()\n` +
        `WHERE $__timeFilter("ts")\n` +
        `  AND entity IN (\${entity:singlequote})\n` +
        `GROUP BY $__timeAlias()\n` +
        `ORDER BY $__timeAlias() DESC\n` +
        `LIMIT 100000;\n` +
        `SET useMultistageEngine=false`,
    });

    // User's `=false` should be preserved as-is; no `=true` line added by the auto-append guard.
    await expect(page.getByTestId('sql-preview')).toContainText(`SET useMultistageEngine=false`);
    await expect(page.getByTestId('sql-preview')).not.toContainText(`SET useMultistageEngine=true`);
  });

  /**
   * Scenario 10: Variable's backing SQL contains `$__table()` macro — must be expanded when injected.
   * Pre-fix, the macro would land in Pinot unexpanded and fail to parse.
   */
  test('10. Code mode $__table() macro inside variable backing SQL is expanded', async ({ page, datasource }) => {
    await openNewDashboard(page);

    await addPinotQueryVariable(page, datasource.name, {
      name: 'entityWithMacro',
      sql: 'SELECT DISTINCT entity FROM $__table() LIMIT 4000',
      table: 'highCardinality',
      multi: true,
      includeAll: true,
    });

    await selectAllInVariable(page, 'entityWithMacro');

    await addPanelInCodeMode(page, datasource.name, {
      table: 'highCardinality',
      code:
        `SELECT $__timeGroup("ts") AS $__timeAlias(),\n` +
        `  SUM("value") AS $__metricAlias()\n` +
        `FROM $__table()\n` +
        `WHERE $__timeFilter("ts")\n` +
        `  AND entity IN (\${entityWithMacro:singlequote})\n` +
        `GROUP BY $__timeAlias()\n` +
        `ORDER BY $__timeAlias() DESC\n` +
        `LIMIT 100000`,
    });

    // The injected subquery should have $__table() expanded to the actual table name. In Code mode
    // this happens via MacroEngine.ExpandMacros which runs over the entire interpolated SQL.
    await expect(page.getByTestId('sql-preview')).toContainText(`"highCardinality"`);
    await expect(page.getByTestId('sql-preview')).not.toContainText(`$__table`);
    await expect(page.getByTestId('sql-preview')).toContainText(`SET useMultistageEngine=true;`);
  });
});

// ---------------------------------------------------------------------------
// Helpers (mirrored from SubqueryOptimization.spec.ts)
// ---------------------------------------------------------------------------

async function openNewDashboard(page: Page) {
  await page.goto('http://localhost:3000/dashboard/new?orgId=1');
}

async function selectAllInVariable(page: Page, variableName: string) {
  await page.getByRole('button', { name: variableName, exact: true }).click();
  await page.getByText('All', { exact: true }).click();
  await page.keyboard.press('Escape');
}
