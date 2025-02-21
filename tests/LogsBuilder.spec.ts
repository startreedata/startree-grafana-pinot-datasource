import {
  addDashboardConstant,
  checkDropdown,
  checkFilterEditor,
  checkQueryOptionEditor,
  checkRunQueryButton,
  queryEditorTest as test,
  selectDatasource,
  setPanelTimeWindow,
} from '@helpers/helpers';
import { expect } from '@playwright/test';

test.describe('Create Panel with Logs Builder', async () => {
  test.beforeEach(async ({ page, datasource }) => {
    const tablesResponse = page.waitForResponse('/**/resources/tables');

    await page.goto('http://localhost:3000/dashboard/new?orgId=1');
    await page.getByLabel('Add new panel', { exact: true }).click();
    await setPanelTimeWindow(page);
    await selectDatasource(page, datasource.name);
    await page.getByTestId('select-query-type').getByText('PinotQL').click();
    await page.getByTestId('select-editor-mode').getByText('Builder').click();
    await page.getByLabel('toggle-viz-picker').click();
    await page.getByLabel('Plugin visualization item Logs').click();
    await page.getByTestId('select-display-type').getByText('Logs').click();
    await tablesResponse;
  });

  test('Run query button', async ({ page }) => await checkRunQueryButton(page));

  test('Choose table', async ({ page }) =>
    await checkDropdown(page, page.getByTestId('select-table-dropdown'), {
      want: ['complex_website', 'simple_website', 'nginxLogs'],
      setValue: 'nginxLogs',
    }));

  test('Choose log message column', async ({ page }) => {
    await page.getByTestId('select-table-dropdown').click();
    await page.getByLabel('Select options menu').getByText('nginxLogs', { exact: true }).click();

    await checkDropdown(page, page.getByTestId('select-log-column'), {
      want: ['message', 'referrer', 'method', 'uri', 'ipAddr'],
      setValue: 'message',
    });
  });

  test('Choose metadata', async ({ page }) => {
    await page.getByTestId('select-table-dropdown').click();
    await page.getByLabel('Select options menu').getByText('nginxLogs', { exact: true }).click();

    await page.getByTestId('select-log-column').click();
    await page.getByLabel('Select options menu').getByText('message', { exact: true }).click();

    await checkDropdown(page, page.getByTestId('select-metadata'), {
      want: ['referrer', 'method', 'uri', 'ipAddr'],
      dontWant: ['message'],
      setValue: 'referrer',
    });
  });

  test('Edit json extractor', async ({ page }) => {
    await page.getByTestId('select-table-dropdown').click();
    await page.getByLabel('Select options menu').getByText('nginxLogs', { exact: true }).click();

    await page.getByTestId('select-time-column-dropdown').click();
    await page.getByLabel('Select options menu').getByText('ts', { exact: true }).click();

    await page.getByTestId('select-log-column').click();
    await page.getByLabel('Select options menu').getByText('message', { exact: true }).click();

    await page.getByTestId('add-json-extractor-btn').click();
    await checkDropdown(page, page.getByTestId('json-extractor-select-column'), {
      want: ['message', 'referrer', 'method', 'uri', 'ipAddr'],
      setValue: 'referrer',
    });

    await page.getByTestId('json-extractor-input-path').getByRole('textbox').fill('$.key1');
    await expect(page.getByTestId('json-extractor-input-path').getByRole('textbox')).toHaveValue('$.key1');

    await checkDropdown(page, page.getByTestId('json-extractor-select-result-type'), {
      want: ['INT', 'LONG', 'FLOAT', 'DOUBLE', 'BOOLEAN', 'TIMESTAMP', 'STRING'],
      setValue: 'STRING',
    });

    await page.getByTestId('json-extractor-input-alias').getByRole('textbox').fill('json_extractor');
    await expect(page.getByTestId('json-extractor-input-alias').getByRole('textbox')).toHaveValue('json_extractor');
  });

  test('Edit regexp extractor', async ({ page }) => {
    await page.getByTestId('select-table-dropdown').click();
    await page.getByLabel('Select options menu').getByText('nginxLogs', { exact: true }).click();

    await page.getByTestId('select-time-column-dropdown').click();
    await page.getByLabel('Select options menu').getByText('ts', { exact: true }).click();

    await page.getByTestId('select-log-column').click();
    await page.getByLabel('Select options menu').getByText('message', { exact: true }).click();

    await page.getByTestId('add-regexp-extractor-btn').click();
    await checkDropdown(page, page.getByTestId('regexp-extractor-select-column'), {
      want: ['message', 'referrer', 'method', 'uri', 'ipAddr'],
      setValue: 'referrer',
    });

    await page.getByTestId('regexp-extractor-input-pattern').getByRole('textbox').fill('(.*)');
    await expect(page.getByTestId('regexp-extractor-input-pattern').getByRole('textbox')).toHaveValue('(.*)');

    await checkDropdown(page, page.getByTestId('regexp-extractor-select-group'), {
      want: ['0', '1'],
      setValue: '1',
    });

    await page.getByTestId('regexp-extractor-input-alias').getByRole('textbox').fill('regexp_extractor');
    await expect(page.getByTestId('regexp-extractor-input-alias').getByRole('textbox')).toHaveValue('regexp_extractor');
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

  test('Logs render when minimum fields are used', async ({ page }) => {
    await page.getByTestId('select-table-dropdown').click();
    await page.getByLabel('Select options menu').getByText('nginxLogs', { exact: true }).click();

    await page.getByTestId('select-time-column-dropdown').click();
    await page.getByLabel('Select options menu').getByText('ts', { exact: true }).click();

    const dataQueryResponse = page.waitForResponse('/api/ds/query');
    await page.getByTestId('select-log-column').click();
    await page.getByLabel('Select options menu').getByText('message', { exact: true }).click();

    await dataQueryResponse;
    await expect(page.getByRole('rowgroup')).toContainText(
      '143.110.222.166 - - [06/Nov/2024:21:06:58 +0000] "GET / HTTP/1.1" 403 134 "-" "Mozilla/5.0 (iPhone; CPU iPhone OS 16_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Mobile/15E148 Safari/604.1" "-"'
    );
  });

  test('Logs renders when all fields are used', async ({ page }) => {
    await page.getByTestId('select-table-dropdown').click();
    await page.getByLabel('Select options menu').getByText('nginxLogs', { exact: true }).click();

    await page.getByTestId('select-time-column-dropdown').click();
    await page.getByLabel('Select options menu').getByText('ts', { exact: true }).click();

    await page.getByTestId('select-log-column').click();
    await page.getByLabel('Select options menu').getByText('message', { exact: true }).click();

    await page.getByTestId('select-metadata-dropdown').click();
    await page.getByLabel('Select options menu').getByText('method', { exact: true }).click();

    await page.getByTestId('add-regexp-extractor-btn').click();
    await page.getByTestId('regexp-extractor-select-column').getByRole('img').click();
    await page.getByLabel('Select options menu').getByText('message').click();
    await page.getByTestId('regexp-extractor-input-pattern').getByRole('textbox').fill('GET .* (HTTP/\\d\\.\\d)');
    await page.getByTestId('regexp-extractor-select-group').getByRole('img').click();
    await page.getByLabel('Select options menu').getByText('1').click();
    await page.getByTestId('regexp-extractor-input-alias').getByRole('textbox').fill('http_ver');

    await page.getByTestId('add-query-option-btn').click();
    await page.getByTestId('select-query-option-name').click();
    await page.getByLabel('Select options menu').getByText('timeoutMs', { exact: true }).click();
    await page.getByTestId('input-query-option-value').getByRole('textbox').fill('1000');

    await expect(page.getByTestId('sql-preview')).toContainText(
      //language=text
      `SELECT
    "message" AS '__message',
    "method",
    REGEXPEXTRACT("message", 'GET .* (HTTP/\\d\\.\\d)', 1, '') AS 'http_ver',
    "ts"
FROM "nginxLogs"
WHERE "message" IS NOT NULL
    AND "ts" >= 1672531200000 AND "ts" < 1735689600999
ORDER BY
    "ts" ASC,
    "__message" ASC
LIMIT 100000;`
    );

    await expect(page.getByRole('rowgroup')).toContainText(
      '143.110.222.166 - - [06/Nov/2024:21:06:58 +0000] "GET / HTTP/1.1" 403 134 "-" "Mozilla/5.0 (iPhone; CPU iPhone OS 16_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Mobile/15E148 Safari/604.1" "-"'
    );

    await page.getByText('- - [06/Nov/2024:21:06:58 +0000] "GET / HTTP/1.1" 403 134 "-" "Mozilla/5.0 (').click();
    await expect(page.getByLabel('Panel Title panel')).toContainText(
      'Log labelshttp_verHTTP/1.1methodGETDetected fieldsTime1730927218000'
    );
  });

  test('Logs render when using dashboard variables', async ({ page }) => {
    await page.getByTestId('select-table-dropdown').click();
    await page.getByLabel('Select options menu').getByText('nginxLogs', { exact: true }).click();

    await page.getByTestId('select-time-column-dropdown').click();
    await page.getByLabel('Select options menu').getByText('ts', { exact: true }).click();

    await addDashboardConstant(page, 'logLine', 'message');
    await page.getByTestId('select-log-column').click();
    await page.keyboard.type('$logLine');
    await page.keyboard.press('Enter');

    await addDashboardConstant(page, 'metadata', 'method');
    await page.getByTestId('select-metadata-dropdown').click();
    await page.keyboard.type('$metadata');
    await page.keyboard.press('Enter');

    await page.getByTestId('add-regexp-extractor-btn').click();

    await addDashboardConstant(page, 'regexpColumn', 'message');
    await page.getByTestId('regexp-extractor-select-column').getByRole('img').click();
    await page.keyboard.type('$regexpColumn');
    await page.keyboard.press('Enter');

    await page.getByTestId('regexp-extractor-input-pattern').getByRole('textbox').fill('GET .* (HTTP/\\d\\.\\d)');

    await page.getByTestId('regexp-extractor-select-group').getByRole('img').click();
    await page.getByLabel('Select options menu').getByText('1').click();

    await addDashboardConstant(page, 'regexpAlias', 'http_ver');
    await page.getByTestId('regexp-extractor-input-alias').getByRole('textbox').fill('$regexpAlias');

    await page.getByTestId('add-query-option-btn').click();

    await addDashboardConstant(page, 'queryOptionName', 'timeoutMs');
    await page.getByTestId('select-query-option-name').click();
    await page.keyboard.type('$queryOptionName');
    await page.keyboard.press('Enter');

    await addDashboardConstant(page, 'queryOptionValue', '100');
    await page.getByTestId('input-query-option-value').getByRole('textbox').fill('$queryOptionValue');

    await expect(page.getByTestId('sql-preview')).toContainText(
      //language=text
      `SELECT
    "message" AS '__message',
    "method",
    REGEXPEXTRACT("message", 'GET .* (HTTP/\\d\\.\\d)', 1, '') AS 'http_ver',
    "ts"
FROM "nginxLogs"
WHERE "message" IS NOT NULL
    AND "ts" >= 1672531200000 AND "ts" < 1735689600999
ORDER BY
    "ts" ASC,
    "__message" ASC
LIMIT 100000;`
    );

    await expect(page.getByRole('rowgroup')).toContainText(
      '143.110.222.166 - - [06/Nov/2024:21:06:58 +0000] "GET / HTTP/1.1" 403 134 "-" "Mozilla/5.0 (iPhone; CPU iPhone OS 16_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Mobile/15E148 Safari/604.1" "-"'
    );

    await page.getByText('- - [06/Nov/2024:21:06:58 +0000] "GET / HTTP/1.1" 403 134 "-" "Mozilla/5.0 (').click();
    await expect(page.getByLabel('Panel Title panel')).toContainText(
      'Log labelshttp_verHTTP/1.1methodGETDetected fieldsTime1730927218000'
    );
  });
});
