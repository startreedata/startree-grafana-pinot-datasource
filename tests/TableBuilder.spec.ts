import { expect, Page } from '@playwright/test';
import {
  checkDropdown,
  checkFilterEditor,
  checkQueryOptionEditor,
  checkRunQueryButton,
  queryEditorTest as test,
  selectDatasource,
  setExploreTimeWindow,
  setPanelTimeWindow,
} from '@helpers/helpers';

test.describe('Create Panel with Table Builder', async () => {
  test.beforeEach(async ({ page, datasource }) => {
    const tablesResponse = page.waitForResponse('/**/resources/tables');

    await page.goto('http://localhost:3000/dashboard/new?orgId=1');
    await page.getByLabel('Add new panel', { exact: true }).click();
    await setPanelTimeWindow(page);
    await selectDatasource(page, datasource.name);
    await page.getByTestId('select-query-type').getByText('PinotQL').click();
    await page.getByTestId('select-editor-mode').getByText('Builder').click();
    await page.getByLabel('toggle-viz-picker').click();
    await page.getByLabel('Plugin visualization item Table').click();
    await page.getByTestId('select-display-type').getByText('Table', { exact: true }).click();
    await tablesResponse;
  });

  test('Run query button', async ({ page }) => await checkRunQueryButton(page));

  test('Choose table', async ({ page }) =>
    await checkDropdown(page, page.getByTestId('select-table-dropdown'), {
      want: ['complex_website', 'simple_website', 'nginxLogs'],
      setValue: 'complex_website',
    }));

  test('Choose dimensions', async ({ page }) => {
    await checkDimensionsDropdown(page);
  });

  test('Edit aggregations', async ({ page }) => {
    await checkAggregationsEditor(page);
  });

  test('Choose order by', async ({ page }) => {
    await checkOrderByDropdown(page);
  });

  test('Edit filters', async ({ page }) => {
    await checkFilterEditor(page);
  });

  test('Edit query options', async ({ page }) => {
    await checkQueryOptionEditor(page);
  });

  test('Set limit', async ({ page }) => {
    await page.getByTestId('input-limit').getByRole('textbox').fill('100');
    await expect(page.getByTestId('input-limit').getByRole('textbox')).toHaveValue('100');
  });

  test('Defaults to a COUNT(*) aggregation', async ({ page }) => {
    await checkTableRendersDefault(page);
  });

  test('Table renders when all fields are used', async ({ page }) => {
    await checkTableRendersAllFields(page);
  });
});

test.describe('Explore with Table Builder', async () => {
  test.beforeEach(async ({ page, datasource }) => {
    const tablesResponse = page.waitForResponse('/**/resources/tables');

    await page.goto('/explore');
    await setExploreTimeWindow(page);
    await selectDatasource(page, datasource.name);
    await page.getByTestId('select-query-type').getByText('PinotQL').click();
    await page.getByTestId('select-editor-mode').getByText('Builder').click();
    await page.getByTestId('select-display-type').getByText('Table', { exact: true }).click();
    await tablesResponse;
  });

  test('Choose table', async ({ page }) =>
    await checkDropdown(page, page.getByTestId('select-table-dropdown'), {
      want: ['complex_website', 'simple_website', 'nginxLogs'],
      setValue: 'complex_website',
    }));

  test('Choose dimensions', async ({ page }) => {
    await checkDimensionsDropdown(page);
  });

  test('Edit aggregations', async ({ page }) => {
    await checkAggregationsEditor(page);
  });

  test('Table renders when all fields are used', async ({ page }) => {
    await checkTableRendersAllFields(page);
  });
});

async function selectComplexWebsite(page: Page) {
  const columnsResponse = page.waitForResponse('/**/resources/columns');
  await page.getByTestId('select-table-dropdown').click();
  await page.getByLabel('Select options menu').getByText('complex_website', { exact: true }).click();
  await columnsResponse;
  await page.getByTestId('select-time-column-dropdown').click();
  await page.getByLabel('Select options menu').getByText('hoursSinceEpoch', { exact: true }).click();
}

async function checkDimensionsDropdown(page: Page) {
  await selectComplexWebsite(page);

  await checkDropdown(page, page.getByTestId('select-group-by-dropdown'), {
    want: ['country', 'browser', 'platform', 'clicks', 'errors', 'views'],
    setValue: 'country',
  });
}

async function checkAggregationsEditor(page: Page) {
  await selectComplexWebsite(page);

  // The builder seeds a default COUNT(*) row when there are no dimensions or aggregations yet.
  await expect(page.getByTestId('edit-aggregation')).toHaveCount(1);

  await checkDropdown(page, page.getByTestId('aggregation-select-function').first(), {
    want: ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN'],
    dontWant: ['NONE'],
    setValue: 'SUM',
  });

  await checkDropdown(page, page.getByTestId('aggregation-select-column').first(), {
    want: ['clicks', 'errors', 'views'],
    setValue: 'views',
  });

  // Add a second aggregation, then remove it.
  await page.getByTestId('add-aggregation-btn').click();
  await expect(page.getByTestId('edit-aggregation')).toHaveCount(2);
  await page.getByTestId('delete-aggregation-btn').nth(1).click();
  await expect(page.getByTestId('edit-aggregation')).toHaveCount(1);
}

async function checkOrderByDropdown(page: Page) {
  await selectComplexWebsite(page);

  await page.getByTestId('select-group-by-dropdown').click();
  await page.getByLabel('Select options menu').getByText('country', { exact: true }).click();

  // Default aggregation is COUNT(*); order-by offers dimensions and aggregation result columns.
  await checkDropdown(page, page.getByTestId('select-order-by-dropdown'), {
    want: ['country asc', 'country desc', 'COUNT(*) asc', 'COUNT(*) desc'],
    setValue: 'COUNT(*) desc',
  });
}

async function checkTableRendersDefault(page: Page) {
  await selectComplexWebsite(page);

  await expect(page.getByTestId('sql-preview')).toContainText(
    // language=text
    `SELECT
    COUNT(*) AS "COUNT(*)"
FROM
    "complex_website"
WHERE
    "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482136
LIMIT 100000;`
  );

  await expect(page.getByText('No data')).not.toBeVisible();
}

async function checkTableRendersAllFields(page: Page) {
  await selectComplexWebsite(page);

  await page.getByTestId('select-group-by-dropdown').click();
  await expect(page.getByLabel('Select options menu').getByText('country', { exact: true })).toBeVisible();
  await page.getByLabel('Select options menu').getByText('country', { exact: true }).click();

  // Add a second aggregation (the default COUNT(*) stays as the first), set it to SUM(views).
  await page.getByTestId('add-aggregation-btn').click();
  await page.getByTestId('aggregation-select-function').nth(1).click();
  await expect(page.getByLabel('Select options menu').getByText('SUM', { exact: true })).toBeVisible();
  await page.getByLabel('Select options menu').getByText('SUM', { exact: true }).click();
  await page.getByTestId('aggregation-select-column').nth(1).click();
  await expect(page.getByLabel('Select options menu').getByText('views', { exact: true })).toBeVisible();
  await page.getByLabel('Select options menu').getByText('views', { exact: true }).click();

  await page.getByTestId('select-order-by-dropdown').click();
  await expect(page.getByLabel('Select options menu').getByText('SUM(views) desc', { exact: true })).toBeVisible();
  await page.getByLabel('Select options menu').getByText('SUM(views) desc', { exact: true }).click();

  await page.getByTestId('input-limit').getByRole('textbox').fill('100');

  await page.getByTestId('run-query-btn').click();
  await page.waitForTimeout(500);

  await expect(page.getByTestId('sql-preview')).toContainText(
    // language=text
    `SELECT
    "country",
    COUNT(*) AS "COUNT(*)",
    SUM("views") AS "SUM(views)"
FROM
    "complex_website"
WHERE
    "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482136
GROUP BY
    "country"
ORDER BY
    "SUM(views)" DESC
LIMIT 100;`
  );

  await expect(page.getByText('No data')).not.toBeVisible();
}
