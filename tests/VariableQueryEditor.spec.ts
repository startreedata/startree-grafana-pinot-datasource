import { expect } from '@playwright/test';
import { queryEditorTest as test } from '@helpers/helpers';

test.beforeEach(async ({ page, datasource }) => {
  await page.goto('http://localhost:3000/dashboard/new?orgId=1');
  await page.getByLabel('Dashboard settings').click();
  await page.getByRole('link', { name: 'Variables' }).click();
  await page.getByTestId('data-testid Call to action button Add variable').click();

  const tablesResponse = page.waitForResponse('/**/resources/tables');
  await page.getByLabel('Data source picker select').click();
  await page.getByText(datasource.name).click();
  await tablesResponse;
});

test('Tables', async ({ page }) => {
  await page.getByText('Tables').click();
  for (const text of ['complex_website', 'simple_website', 'nginxLogs']) {
    await expect(page.getByText(text, { exact: true })).toBeVisible();
  }
});

test('Columns', async ({ page }) => {
  await page.getByText('Columns').click();

  const dataQueryResponse = page.waitForResponse('/api/ds/query');
  await page.getByTestId('select-table-dropdown').click();
  await page.getByText('complex_website', { exact: true }).click();

  await dataQueryResponse;
  for (const text of ['hoursSinceEpoch', 'views', 'clicks', 'errors', 'country', 'browser', 'platform']) {
    await expect(page.getByText(text, { exact: true })).toBeVisible();
  }
});

test('Distinct Values', async ({ page }) => {
  await page.getByText('Distinct Values').click();

  const columnsResponse = page.waitForResponse('/**/resources/columns');
  const sqlPreviewResponse = page.waitForResponse('/**/resources/preview/sql/distinctValues');
  const dataQueryResponse = page.waitForResponse('/api/ds/query');

  await page.getByTestId('select-table-dropdown').click();
  await page.getByText('complex_website', { exact: true }).click();

  await columnsResponse;
  await page.getByTestId('select-column-dropdown').click();
  await page.getByText('browser', { exact: true }).click();

  await sqlPreviewResponse;
  await expect(page.getByTestId('sql-preview-value')).toContainText(
    //language=text
    'SELECT DISTINCT "browser" FROM "complex_website" WHERE "browser" IS NOT NULL ORDER BY "browser" ASC LIMIT 100;'
  );

  await dataQueryResponse;
  for (const text of ['chrome', 'edge', 'firefox', 'ie', 'safari']) {
    await expect(page.getByText(text, { exact: true })).toBeVisible();
  }
});

test('Sql Code', async ({ page }) => {
  await page.getByText('Sql Query').click();

  const dataQueryResponse = page.waitForResponse('/api/ds/query');

  const codebox = page.getByTestId('sql-editor-content').getByRole('code');
  await codebox.click();
  await page.keyboard.press('ControlOrMeta+a');
  await page.keyboard.press('ControlOrMeta+x');
  await page.keyboard.type(
    // language=text
    `SELECT DISTINCT "browser" FROM "complex_website" WHERE "browser" IS NOT NULL ORDER BY "browser" ASC LIMIT 100;`
  );

  await dataQueryResponse;
  for (const text of ['chrome', 'edge', 'firefox', 'ie', 'safari']) {
    await expect(page.getByText(text, { exact: true })).toBeVisible();
  }
});
