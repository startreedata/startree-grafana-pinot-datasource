import { expect, Page } from '@playwright/test';
import {
  addPinotQueryVariable,
  queryEditorTest as test,
  selectDatasource,
} from '@helpers/helpers';

/**
 * Subquery optimization e2e tests — Logs (builder) mode.
 *
 * Logs builder mode constructs SQL via the same DimensionFilters path as the time-series
 * builder, so the filter interpolation paths in PR #184 apply here too:
 *   - High-cardinality (>1000) `=` filter → subquery + auto-MSE (via useAutoSurfaceMultiStageEngine)
 *   - Low-cardinality multi-select → IN literals expansion
 *   - Variable backing SQL with `$__table()` → macro expanded by ExpandSubqueryMacros (logs path
 *     does not load tableConfigs but still supports $__table, $__timeFilter, etc.)
 *
 * Uses the `highCardinality` table (provisioned earlier) for all three tests — provides both a
 * high-cardinality column (`entity`, 1500 distinct) and a low-cardinality column (`category`, 6).
 * `entity` is also used as the "log message" column.
 */
test.describe('Logs mode subquery optimization', () => {
  /**
   * Scenario 11: Logs builder filter var ALL >1000 → subquery + MSE auto-injected.
   * Verifies useAutoSurfaceMultiStageEngine wires correctly into PinotQlLogsBuilder.
   */
  test('11. Logs builder, ALL >1000 → subquery + MSE auto-injected', async ({ page, datasource }) => {
    await openNewDashboard(page);

    await addPinotQueryVariable(page, datasource.name, {
      name: 'entity',
      sql: 'SELECT DISTINCT entity FROM highCardinality LIMIT 4000',
      multi: true,
      includeAll: true,
    });

    await selectAllInVariable(page, 'entity');

    await addPanelInLogsBuilder(page, datasource.name, {
      table: 'highCardinality',
      timeColumn: 'ts',
      logColumn: 'entity',
      filterColumn: 'entity',
      operator: '=',
      filterValue: '$entity',
    });

    await expect(page.getByTestId('sql-preview')).toContainText(
      `("entity" in (SELECT DISTINCT entity FROM highCardinality LIMIT 4000))`
    );
    await expect(page.getByTestId('sql-preview')).toContainText(`SET useMultistageEngine=true;`);
  });

  /**
   * Scenario 12: Logs builder filter var partial selection → IN literals (no MSE).
   */
  test('12. Logs builder, partial selection → IN literals (no MSE)', async ({ page, datasource }) => {
    await openNewDashboard(page);

    await addPinotQueryVariable(page, datasource.name, {
      name: 'category',
      sql: 'SELECT DISTINCT category FROM highCardinality',
      multi: true,
      includeAll: false,
    });

    await selectVariableValues(page, 'category', ['cache', 'api']);

    await addPanelInLogsBuilder(page, datasource.name, {
      table: 'highCardinality',
      timeColumn: 'ts',
      logColumn: 'entity',
      filterColumn: 'category',
      operator: '=',
      filterValue: '$category',
    });

    await expect(page.getByTestId('sql-preview')).toContainText(`("category" in ('cache', 'api'))`);
    await expect(page.getByTestId('sql-preview')).not.toContainText(`SET useMultistageEngine=true;`);
  });

  /**
   * Scenario 13: Variable backing SQL with `$__table()` macro is expanded when injected as
   * subquery in logs mode. Tests that the logs builder's ExpandSubqueryMacros call (without
   * tableConfigs) still handles the common `$__table()` macro correctly.
   */
  test('13. Logs builder, $__table() macro inside variable backing SQL is expanded', async ({ page, datasource }) => {
    await openNewDashboard(page);

    await addPinotQueryVariable(page, datasource.name, {
      name: 'entityWithMacro',
      sql: 'SELECT DISTINCT entity FROM $__table() LIMIT 4000',
      table: 'highCardinality',
      multi: true,
      includeAll: true,
    });

    await selectAllInVariable(page, 'entityWithMacro');

    await addPanelInLogsBuilder(page, datasource.name, {
      table: 'highCardinality',
      timeColumn: 'ts',
      logColumn: 'entity',
      filterColumn: 'entity',
      operator: '=',
      filterValue: '$entityWithMacro',
    });

    // The injected subquery should have $__table() expanded to the panel's table name.
    await expect(page.getByTestId('sql-preview')).toContainText(`"highCardinality"`);
    await expect(page.getByTestId('sql-preview')).not.toContainText(`$__table`);
    await expect(page.getByTestId('sql-preview')).toContainText(`SET useMultistageEngine=true;`);
  });
});

// ---------------------------------------------------------------------------
// Helpers (local to this spec; promote later if reused in another spec)
// ---------------------------------------------------------------------------

async function openNewDashboard(page: Page) {
  await page.goto('http://localhost:3000/dashboard/new?orgId=1');
}

async function selectAllInVariable(page: Page, variableName: string) {
  await page.getByRole('button', { name: variableName, exact: true }).click();
  await page.getByText('All', { exact: true }).click();
  await page.keyboard.press('Escape');
}

async function selectVariableValues(page: Page, variableName: string, values: string[]) {
  await page.getByRole('button', { name: variableName, exact: true }).click();
  for (const value of values) {
    // Use .check() (idempotent) instead of .click() (toggles) — if Grafana pre-selected this
    // value as the default, .click() would toggle it OFF, then Escape would revert and the test
    // would silently lose the explicit selection. .check() ensures the box ends up checked.
    await page.getByRole('checkbox', { name: value, exact: true }).check();
  }
  await page.keyboard.press('Escape');
}

/**
 * Adds a panel using the PinotQL **Logs builder** display type. Switches the visualization
 * picker to Logs, selects table + time column + log message column, then adds a single
 * filter referencing a variable.
 */
async function addPanelInLogsBuilder(
  page: Page,
  datasourceName: string,
  opts: { table: string; timeColumn: string; logColumn: string; filterColumn: string; operator: string; filterValue: string }
) {
  await page.getByLabel('Add new panel', { exact: true }).click();
  // Arm the /tables wait BEFORE selecting the datasource.
  const tablesResponse = page.waitForResponse('/**/resources/tables');
  await selectDatasource(page, datasourceName);
  await tablesResponse;

  await page.getByTestId('select-query-type').getByText('PinotQL').click();
  await page.getByTestId('select-editor-mode').getByText('Builder').click();

  // Switch the panel's visualization to Logs (mirrors LogsBuilder.spec.ts).
  await page.getByLabel('toggle-viz-picker').click();
  await page.getByLabel('Plugin visualization item Logs').click();
  await page.getByTestId('select-display-type').getByText('Logs').click();

  await page.getByTestId('select-table-dropdown').click();
  await page.getByLabel('Select options menu').getByText(opts.table, { exact: true }).click();

  await page.getByTestId('select-time-column-dropdown').click();
  await page.getByLabel('Select options menu').getByText(opts.timeColumn, { exact: true }).click();

  await page.getByTestId('select-log-column').click();
  await page.getByLabel('Select options menu').getByText(opts.logColumn, { exact: true }).click();

  // Add the variable-backed filter.
  await page.getByTestId('add-filter-btn').click();
  await page.getByTestId('select-query-filter-column').click();
  await page.getByLabel('Select options menu').getByText(opts.filterColumn, { exact: true }).click();
  await page.getByTestId('select-query-filter-operator').click();
  await page.getByLabel('Select options menu').getByText(opts.operator, { exact: true }).click();
  await page.getByTestId('select-query-filter-value').click();
  await page.keyboard.type(opts.filterValue);
  await expect(page.getByText('Hit enter to add')).toBeVisible();
  await page.keyboard.press('Enter');
  await page.keyboard.press('Tab');

  await page.getByTestId('run-query-btn').click({ force: true });
}
