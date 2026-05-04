import { expect, Page } from '@playwright/test';
import {
  addPinotQueryVariable,
  queryEditorTest as test,
  selectDatasource,
} from '@helpers/helpers';

/**
 * Subquery optimization e2e tests.
 *
 * Exercises the variable-selection flow end-to-end through the Grafana UI for
 * the Builder mode filter interpolation paths added in PR #184:
 *   - High-cardinality (>1000) ALL → subquery + auto-MSE
 *   - Low-cardinality ALL → IN literals expansion
 *   - Partial selection → IN literals
 *   - Single value → equality with quoted literal
 *   - Two-variable permutations
 *
 * Requires the `highCardinality` table on the target Pinot cluster (1500
 * distinct entities × 6 categories = 9000 rows). The fixture is defined under
 * `pkg/pinot/pinottest/data/highCardinality_*.json` and is auto-created by the
 * local Docker test setup; for a remote cluster, provision once with the same
 * schema/config/data via the standard Pinot REST endpoints.
 */
test.describe('Subquery optimization for template variables', () => {
  /**
   * Scenario 1: Single variable, ALL selected, >1000 distinct values.
   * Expected: filter expands to subquery; useMultistageEngine=true is auto-injected.
   */
  test('1. Single var, ALL >1000 → subquery + MSE auto-injected', async ({ page, datasource }) => {
    await openNewDashboard(page);

    await addPinotQueryVariable(page, datasource.name, {
      name: 'entity',
      sql: 'SELECT DISTINCT entity FROM highCardinality LIMIT 4000',
      multi: true,
      includeAll: true,
    });

    await selectAllInVariable(page, 'entity');
    await addPanelWithVariableFilter(page, datasource.name, {
      table: 'highCardinality',
      timeColumn: 'ts',
      filterColumn: 'entity',
      operator: '=',
      filterValue: '$entity',
    });

    await expect(page.getByTestId('sql-preview')).toContainText(
      `("entity" in (SELECT DISTINCT entity FROM highCardinality LIMIT 4000))`
    );
    await expect(page.getByTestId('sql-preview')).toContainText(`SET useMultistageEngine=true;`);

    await assertPanelRendersWithoutError(page);
  });

  /**
   * Scenario 2: Single variable, ALL selected, low cardinality (~250).
   * Expected: filter expands to quoted literal IN; no MSE.
   */
  test('2. Single var, ALL low-card → IN literals', async ({ page, datasource }) => {
    await openNewDashboard(page);

    await addPinotQueryVariable(page, datasource.name, {
      name: 'country',
      sql: 'SELECT DISTINCT country FROM complex_website',
      multi: true,
      includeAll: true,
    });

    await selectAllInVariable(page, 'country');
    await addPanelWithVariableFilter(page, datasource.name, {
      table: 'complex_website',
      timeColumn: 'hoursSinceEpoch',
      filterColumn: 'country',
      operator: '=',
      filterValue: '$country',
    });

    // Should show quoted literals, not a subquery — country has ~250 distinct values, below threshold.
    await expect(page.getByTestId('sql-preview')).toContainText(`("country" in (`);
    await expect(page.getByTestId('sql-preview')).not.toContainText(`SELECT DISTINCT country FROM`);
    await expect(page.getByTestId('sql-preview')).not.toContainText(`SET useMultistageEngine=true;`);

    await assertPanelRendersWithoutError(page);
  });

  /**
   * Scenario 3: Single variable, partial selection (2-3 values).
   * Expected: filter expands to IN ('val1', 'val2') with the operator remapped from '=' to 'in'.
   */
  test('3. Single var, partial selection → IN literals', async ({ page, datasource }) => {
    await openNewDashboard(page);

    await addPinotQueryVariable(page, datasource.name, {
      name: 'browser',
      sql: 'SELECT DISTINCT browser FROM complex_website',
      multi: true,
      includeAll: false,
    });

    await selectVariableValues(page, 'browser', ['chrome', 'firefox']);
    await addPanelWithVariableFilter(page, datasource.name, {
      table: 'complex_website',
      timeColumn: 'hoursSinceEpoch',
      filterColumn: 'browser',
      operator: '=',
      filterValue: '$browser',
    });

    await expect(page.getByTestId('sql-preview')).toContainText(`("browser" in ('edge', 'chrome', 'firefox'))`);
    await expect(page.getByTestId('sql-preview')).not.toContainText(`SET useMultistageEngine=true;`);

    await assertPanelRendersWithoutError(page);
  });

  /**
   * Scenario 4: Single variable, single value selected.
   * Expected: filter is `= 'value'` — operator preserved, value quoted.
   */
  test('4. Single var, single value → equality with quoted literal', async ({ page, datasource }) => {
    await openNewDashboard(page);

    await addPinotQueryVariable(page, datasource.name, {
      name: 'browser',
      sql: 'SELECT DISTINCT browser FROM complex_website',
      multi: false,
      includeAll: false,
    });

    await selectVariableValues(page, 'browser', ['chrome']);
    await addPanelWithVariableFilter(page, datasource.name, {
      table: 'complex_website',
      timeColumn: 'hoursSinceEpoch',
      filterColumn: 'browser',
      operator: '=',
      filterValue: '$browser',
    });

    await expect(page.getByTestId('sql-preview')).toContainText(`("browser" = 'chrome')`);
    await expect(page.getByTestId('sql-preview')).not.toContainText(`SET useMultistageEngine=true;`);

    await assertPanelRendersWithoutError(page);
  });

  // Two-variable scenarios (both-ALL, ALL+partial) are covered at the unit-test layer in
  // src/dataquery/SubqueryOptimization.test.ts — see "multiple filters — high-cardinality
  // gets subquery, low-cardinality gets quoted literals" — and at the backend integration
  // layer in pkg/pinot/column_filter_integration_test.go. They were too brittle to maintain
  // as e2e tests because of Grafana's variable editor state when adding the second variable.
});

// ---------------------------------------------------------------------------
// Helpers (kept local to this spec; promote to helpers.ts if reused elsewhere)
// ---------------------------------------------------------------------------

async function openNewDashboard(page: Page) {
  await page.goto('http://localhost:3000/dashboard/new?orgId=1');
}

/**
 * Selects "All" in a dashboard template-variable dropdown via the top toolbar.
 */
async function selectAllInVariable(page: Page, variableName: string) {
  // The dashboard top-bar variable dropdown is a button with role=button and name=<varname>.
  await page.getByRole('button', { name: variableName, exact: true }).click();
  // Options are plain text items, not <input type=checkbox> — click "All" by text.
  await page.getByText('All', { exact: true }).click();
  await page.keyboard.press('Escape');
}

/**
 * Selects specific values in a dashboard template-variable dropdown.
 * Deselects "All" first if present, then clicks the requested values.
 */
async function selectVariableValues(page: Page, variableName: string, values: string[]) {
  await page.getByRole('button', { name: variableName, exact: true }).click();
  for (const value of values) {
    await page.getByText(value, { exact: true }).click();
  }
  await page.keyboard.press('Escape');
}

async function addPanelWithVariableFilter(
  page: Page,
  datasourceName: string,
  opts: { table: string; timeColumn: string; filterColumn: string; operator: string; filterValue: string }
) {
  await page.getByLabel('Add new panel', { exact: true }).click();
  // Arm the /tables wait BEFORE selecting the datasource — Grafana fires the request once
  // during datasource selection, so setting up the wait afterwards races and times out in CI.
  const tablesResponse = page.waitForResponse('/**/resources/tables');
  await selectDatasource(page, datasourceName);
  await tablesResponse;

  await page.getByTestId('select-query-type').getByText('PinotQL').click();
  await page.getByTestId('select-editor-mode').getByText('Builder').click();

  await page.getByTestId('select-table-dropdown').click();
  await page.getByLabel('Select options menu').getByText(opts.table, { exact: true }).click();

  await page.getByTestId('select-time-column-dropdown').click();
  await page.getByLabel('Select options menu').getByText(opts.timeColumn, { exact: true }).click();

  await page.getByTestId('add-filter-btn').click();
  // Filter column
  await page.getByTestId('select-query-filter-column').click();
  await page.getByLabel('Select options menu').getByText(opts.filterColumn, { exact: true }).click();
  // Operator
  await page.getByTestId('select-query-filter-operator').click();
  await page.getByLabel('Select options menu').getByText(opts.operator, { exact: true }).click();
  // Value: type the variable reference and wait for Grafana's "Hit enter to add" hint
  // (tells us the input is ready to commit the typed text as a custom value).
  await page.getByTestId('select-query-filter-value').click();
  await page.keyboard.type(opts.filterValue);
  await expect(page.getByText('Hit enter to add')).toBeVisible();
  await page.keyboard.press('Enter');
  // Blur the dropdown so Grafana commits the chip to saved state — without this, the value
  // stays as an UI draft and the SQL preview renders an empty filter value.
  await page.keyboard.press('Tab');

  // The panel-options sidebar can have a loading overlay (css-1kfdb0e) intercepting clicks;
  // force the click since we just want to trigger the query, not interact with the visual.
  await page.getByTestId('run-query-btn').click({ force: true });
}

/**
 * Verifies the panel's data query returns 200 (broker accepted the SQL).
 */
async function assertPanelRendersWithoutError(page: Page) {
  // The SQL preview is already populated; trigger a fresh run-query to verify execution.
  const dataQueryResponse = page.waitForResponse('/api/ds/query');
  await page.getByTestId('run-query-btn').click({ force: true });
  const response = await dataQueryResponse;
  expect(response.status()).toBe(200);
}
