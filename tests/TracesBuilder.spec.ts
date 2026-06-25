import {
  checkDropdown,
  checkFilterEditor,
  checkQueryOptionEditor,
  checkRunQueryButton,
  queryEditorTest as test,
  selectDatasource,
  setPanelTimeWindow,
} from '@helpers/helpers';
import { expect } from '@playwright/test';

test.describe('Create Panel with Traces Builder', async () => {
  test.beforeEach(async ({ page, datasource }) => {
    const tablesResponse = page.waitForResponse('/**/resources/tables');

    await page.goto('http://localhost:3000/dashboard/new?orgId=1');
    await page.getByLabel('Add new panel', { exact: true }).click();
    await setPanelTimeWindow(page);
    await selectDatasource(page, datasource.name);
    await page.getByTestId('select-query-type').getByText('PinotQL').click();
    await page.getByTestId('select-editor-mode').getByText('Builder').click();
    await page.getByTestId('select-display-type').getByText('Traces').click();
    await tablesResponse;
  });

  test('Run query button', async ({ page }) => await checkRunQueryButton(page));

  test('Choose table', async ({ page }) =>
    await checkDropdown(page, page.getByTestId('select-table-dropdown'), {
      want: ['nginxLogs'],
      setValue: 'nginxLogs',
    }));

  test('Choose trace id column', async ({ page }) => {
    await page.getByTestId('select-table-dropdown').click();
    await page.getByLabel('Select options menu').getByText('nginxLogs', { exact: true }).click();

    await checkDropdown(page, page.getByTestId('select-trace-id-column'), {
      want: ['message', 'referrer', 'method', 'uri', 'ipAddr'],
      setValue: 'message',
    });
  });

  test('Choose duration column', async ({ page }) => {
    await page.getByTestId('select-table-dropdown').click();
    await page.getByLabel('Select options menu').getByText('nginxLogs', { exact: true }).click();

    // The duration picker only offers numeric columns.
    await checkDropdown(page, page.getByTestId('select-duration-column'), {
      want: ['bytesSent', 'status'],
      dontWant: ['message'],
      setValue: 'bytesSent',
    });
  });

  test('Choose duration unit', async ({ page }) => {
    await page.getByTestId('select-duration-unit').getByText('ns', { exact: true }).click();
    await expect(page.getByTestId('select-duration-unit')).toContainText('ns');
  });

  test('Find trace by id input', async ({ page }) => {
    await page.getByTestId('input-trace-id').getByRole('textbox').fill('abc123');
    await expect(page.getByTestId('input-trace-id').getByRole('textbox')).toHaveValue('abc123');
  });

  test('Edit filters', async ({ page }) => {
    await checkFilterEditor(page);
  });

  test('Edit query options', async ({ page }) => {
    await checkQueryOptionEditor(page);
  });

  test('SQL preview reflects the span column mapping', async ({ page }) => {
    await page.getByTestId('select-table-dropdown').click();
    await page.getByLabel('Select options menu').getByText('nginxLogs', { exact: true }).click();

    await page.getByTestId('select-time-column-dropdown').click();
    await page.getByLabel('Select options menu').getByText('ts', { exact: true }).click();

    await page.getByTestId('select-trace-id-column').click();
    await page.getByLabel('Select options menu').getByText('message', { exact: true }).click();

    await page.getByTestId('select-span-id-column').click();
    await page.getByLabel('Select options menu').getByText('referrer', { exact: true }).click();

    await page.getByTestId('select-duration-column').click();
    await page.getByLabel('Select options menu').getByText('bytesSent', { exact: true }).click();

    // Search mode (no trace id): the projection aliases each mapped column to its trace field, the
    // time filter is expanded, and spans are ordered most-recent-first.
    const preview = page.getByTestId('sql-preview');
    await expect(preview).toContainText(`"message" AS 'traceID'`);
    await expect(preview).toContainText(`"referrer" AS 'spanID'`);
    await expect(preview).toContainText(`"ts" AS 'startTime'`);
    await expect(preview).toContainText(`"bytesSent" AS 'duration'`);
    await expect(preview).toContainText(`FROM "nginxLogs"`);
    await expect(preview).toContainText(`ORDER BY "ts" DESC`);
  });

  test('SQL preview filters by trace id in find-by-id mode', async ({ page }) => {
    await page.getByTestId('select-table-dropdown').click();
    await page.getByLabel('Select options menu').getByText('nginxLogs', { exact: true }).click();

    await page.getByTestId('select-time-column-dropdown').click();
    await page.getByLabel('Select options menu').getByText('ts', { exact: true }).click();

    await page.getByTestId('select-trace-id-column').click();
    await page.getByLabel('Select options menu').getByText('message', { exact: true }).click();

    await page.getByTestId('select-span-id-column').click();
    await page.getByLabel('Select options menu').getByText('referrer', { exact: true }).click();

    await page.getByTestId('select-duration-column').click();
    await page.getByLabel('Select options menu').getByText('bytesSent', { exact: true }).click();

    await page.getByTestId('input-trace-id').getByRole('textbox').fill('abc123');

    const preview = page.getByTestId('sql-preview');
    await expect(preview).toContainText(`AND ("message" = 'abc123')`);
    await expect(preview).toContainText(`ORDER BY "ts" ASC`);
  });
});
