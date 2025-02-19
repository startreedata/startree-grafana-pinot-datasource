import { expect, test } from '@playwright/test';
import { Env, randomDatasourceName, setExploreTimeWindow } from '@helpers/helpers';

test.describe('Add Pinot Datasource', async () => {
  // Grafana generates an incremental name for each datasource and saves it when the editor page loads.
  // This causes race condition when we open the new datasource page concurrently, so run tests serially.
  test.describe.configure({ mode: 'serial' });

  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3000/datasources/new');
    await page.getByLabel('Add data source Pinot').click();
    await page.getByPlaceholder('Name').click();
    await page.getByPlaceholder('Name').fill(randomDatasourceName());
  });

  test.afterEach(async ({ page }) => {
    await page.getByLabel('Data source settings page Delete button').click();
    await page.getByLabel('Confirm Modal Danger Button').click();
  });

  test('Valid credentials is successful', async ({ page }) => {
    await page.getByPlaceholder('Controller URL').fill(Env.PinotConnectionControllerUrl);
    await page.getByPlaceholder('Broker URL').fill(Env.PinotConnectionBrokerUrl);
    await page.getByPlaceholder('default').fill(Env.PinotConnectionDatabase);
    await page.getByTestId('select-pinot-token-type').click();
    await page.getByLabel('Select options menu').getByText('Bearer').click();
    await page.getByPlaceholder('Token').fill(Env.PinotConnectionAuthToken);

    const healthCheckResponse = page.waitForResponse('http://localhost:3000/api/datasources/*/health');
    await page.getByLabel('Data source settings page Save and Test button').click();
    await healthCheckResponse;
    await expect(page.getByText('Pinot data source is working')).toBeVisible();
  });

  test('Query Options set at data source are visible in query editor', async ({ page }) => {
    await page.getByPlaceholder('Controller URL').fill(Env.PinotConnectionControllerUrl);
    await page.getByPlaceholder('Broker URL').fill(Env.PinotConnectionBrokerUrl);
    await page.getByPlaceholder('default').fill(Env.PinotConnectionDatabase);
    await page.getByTestId('select-pinot-token-type').click();
    await page.getByLabel('Select options menu').getByText('Bearer').click();
    await page.getByPlaceholder('Token').fill(Env.PinotConnectionAuthToken);
    await page.getByTestId('add-query-option-btn').click();
    await page.getByTestId('select-query-option-name').click();
    await page.getByLabel('Select options menu').getByText('timeoutMs', { exact: true }).click();
    await page.getByTestId('input-query-option-value').getByRole('textbox').click();
    await page.getByTestId('input-query-option-value').getByRole('textbox').fill('100');
    await page.locator('body').click();

    const healthCheckResponse = page.waitForResponse('http://localhost:3000/api/datasources/*/health');
    await page.getByLabel('Data source settings page Save and Test button').click();
    await healthCheckResponse;
    await expect(page.getByText('Pinot data source is working')).toBeVisible();
    const datasourceUrl = page.url();

    await page.getByRole('main').getByRole('link', { name: 'Explore' }).click();
    await setExploreTimeWindow(page);
    const sqlPreviewResponse = page.waitForResponse('/**/resources/preview/sql/builder');
    await page.getByTestId('select-table-dropdown').click();
    await page.getByLabel('Select options menu').getByText('complex_website', { exact: true }).click();
    await sqlPreviewResponse;
    await expect(page.getByTestId('sql-preview')).toContainText(
      // language=text
      `SELECT
    DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '12:HOURS') AS "__time",
    SUM("views") AS "__metric"
FROM
    "complex_website"
WHERE
    "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482148
GROUP BY
    "__time"
ORDER BY
    "__time" DESC
LIMIT 100000;

SET timeoutMs=100;`
    );

    await page.goto(datasourceUrl);
  });

  test('Invalid controller url shows error', async ({ page }) => {
    await page.getByPlaceholder('Controller URL').fill('not a url');
    await page.getByPlaceholder('Broker URL').fill(Env.PinotConnectionBrokerUrl);
    await page.getByPlaceholder('default').fill(Env.PinotConnectionDatabase);
    await page.getByTestId('select-pinot-token-type').click();
    await page.getByLabel('Select options menu').getByText('Bearer').click();
    await page.getByPlaceholder('Token').fill(Env.PinotConnectionAuthToken);

    const healthCheckResponse = page.waitForResponse('http://localhost:3000/api/datasources/*/health');
    await page.getByLabel('Data source settings page Save and Test button').click();
    await healthCheckResponse;
    await expect(page.getByTestId('data-testid Alert error')).toBeVisible();
  });

  test('Invalid broker url shows error', async ({ page }) => {
    await page.getByPlaceholder('Controller URL').fill(Env.PinotConnectionBrokerUrl);
    await page.getByPlaceholder('Broker URL').fill('not a url');
    await page.getByPlaceholder('default').fill(Env.PinotConnectionDatabase);
    await page.getByTestId('select-pinot-token-type').click();
    await page.getByLabel('Select options menu').getByText('Bearer').click();
    await page.getByPlaceholder('Token').fill(Env.PinotConnectionAuthToken);

    const healthCheckResponse = page.waitForResponse('http://localhost:3000/api/datasources/*/health');
    await page.getByLabel('Data source settings page Save and Test button').click();
    await healthCheckResponse;
    await expect(page.getByTestId('data-testid Alert error')).toBeVisible();
  });

  test('Invalid credentials shows error', async ({ page }) => {
    await page.getByPlaceholder('Controller URL').fill(Env.PinotConnectionControllerUrl);
    await page.getByPlaceholder('Broker URL').fill(Env.PinotConnectionBrokerUrl);
    await page.getByPlaceholder('default').fill(Env.PinotConnectionDatabase);
    await page.getByTestId('select-pinot-token-type').click();
    await page.getByLabel('Select options menu').getByText('Bearer').click();
    await page.getByPlaceholder('Token').fill('not a token');

    const healthCheckResponse = page.waitForResponse('http://localhost:3000/api/datasources/*/health');
    await page.getByLabel('Data source settings page Save and Test button').click();
    await healthCheckResponse;
    await expect(page.getByTestId('data-testid Alert error')).toBeVisible();
  });

  test('OAuth Passthrough is enabled', async ({ page }) => {
    // TODO: Log in to Grafana with OAuth

    await page.getByPlaceholder('Controller URL').fill(Env.PinotConnectionControllerUrl);
    await page.getByPlaceholder('Broker URL').fill(Env.PinotConnectionBrokerUrl);
    await page.getByPlaceholder('default').fill(Env.PinotConnectionDatabase);
    await page.getByTestId('switch-oauth-pass-thru').click();

    const healthCheckResponse = page.waitForResponse('http://localhost:3000/api/datasources/*/health');
    await page.getByLabel('Data source settings page Save and Test button').click();
    await healthCheckResponse;
    await expect(page.getByText('Pinot data source is working')).toBeVisible();
  });
});
