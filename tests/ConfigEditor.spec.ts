import { expect, test } from '@playwright/test';
import { Env, randomDatasourceName } from '@helpers/helpers';

test.describe('Add Pinot Datasource', async () => {
  // Grafana generates an incremental name for each datasource and saves it when the editor page loads.
  // This causes race condition when we open the new datasource page concurrently, so run tests serially.
  test.describe.configure({ mode: 'serial' });

  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3000/datasources/new');
    await page.getByLabel('Add data source Pinot').click();
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
});
