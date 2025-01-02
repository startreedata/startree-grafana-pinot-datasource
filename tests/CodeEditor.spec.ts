import { expect, Page, test as base } from '@playwright/test';
import {
  checkDropdown,
  checkRunQueryButton,
  checkTextForm,
  createDatasource,
  deleteDatasource,
  selectDatasource,
  setExploreTimeWindow,
  setPanelTimeWindow,
} from '@helpers/helpers';

interface TestFixtures {
  datasource: { uid: string; name: string };
}

const test = base.extend<TestFixtures>({
  datasource: async ({ page }, use) => {
    const datasource = await createDatasource();
    await use(datasource);
    await deleteDatasource(datasource.uid);
  },
});

test.describe('Explore with Code Editor', async () => {
  test.beforeEach(async ({ page, datasource }) => {
    const tablesResponse = page.waitForResponse('/**/resources/tables');

    await page.goto('/explore');
    await setExploreTimeWindow(page);
    await selectDatasource(page, datasource.name);
    await page.getByTestId('select-query-type').getByText('PinotQL').click();
    await page.getByTestId('select-editor-mode').getByText('Code').click();
    await tablesResponse;
    await expect(page.getByText('No data')).toBeVisible();
  });

  test('Choose display type', async ({ page }) => {
    for (const text of ['Time Series', 'Table', 'Logs']) {
      await page.getByTestId('select-display-type').getByText(text).click();
      await expect(page.getByTestId('select-display-type').getByText(text)).toBeChecked();
    }
  });

  test('Run query button', async ({ page }) => await checkRunQueryButton(page));

  test('Choose table', async ({ page }) =>
    await checkDropdown(page, page.getByTestId('select-table-dropdown'), {
      want: ['complex_website', 'simple_website', 'nginxLogs'],
      setValue: 'complex_website',
    }));

  test('Set time alias', async ({ page }) =>
    await checkTextForm(page.getByTestId('input-time-alias').getByRole('textbox')));

  test('Modify sql', async ({ page }) => await checkSqlEditor(page));

  test('Set legend', async ({ page }) =>
    await checkTextForm(page.getByTestId('input-metric-legend').getByRole('textbox')));

  test.describe('Visualize time series', async () => {
    test.beforeEach(async ({ page }) => {
      await page.getByTestId('select-display-type').getByText('Time Series').click();
    });

    test('Set metric alias', async ({ page }) =>
      await checkTextForm(page.getByTestId('input-metric-alias').getByRole('textbox')));

    test('Time series renders', async ({ page }) => {
      await checkTimeSeriesRenders(page);
    });
  });

  test.describe('Visualize table', async () => {
    test.beforeEach(async ({ page }) => {
      await page.getByTestId('select-display-type').getByText('Table').click();
    });

    test('Table renders', async ({ page }) => {
      await checkTableRenders(page);
      await expect(page.getByLabel('Explore Table')).toContainText('2024-07-25 08:00:00.000');
    });
  });
});

test.describe('Create Panel with Code Editor', async () => {
  test.beforeEach(async ({ page, datasource }) => {
    const tablesResponse = page.waitForResponse('/**/resources/tables');

    await page.goto('http://localhost:3000/dashboard/new?orgId=1');
    await page.getByLabel('Add new panel', { exact: true }).click();
    await setPanelTimeWindow(page);
    await selectDatasource(page, datasource.name);
    await page.getByTestId('select-query-type').getByText('PinotQL').click();
    await page.getByTestId('select-editor-mode').getByText('Code').click();
    await tablesResponse;
    await expect(page.getByText('No data')).toBeVisible();
  });

  test('Choose display type', async ({ page }) => {
    for (const text of ['Time Series', 'Table', 'Logs']) {
      await page.getByTestId('select-display-type').getByText(text).click();
      await expect(page.getByTestId('select-display-type').getByText(text)).toBeChecked();
    }
  });

  test('Run query button', async ({ page }) => await checkRunQueryButton(page));

  test('Choose table', async ({ page }) =>
    await checkDropdown(page, page.getByTestId('select-table-dropdown'), {
      want: ['complex_website', 'simple_website', 'nginxLogs'],
      setValue: 'complex_website',
    }));

  test('Set time alias', async ({ page }) =>
    await checkTextForm(page.getByTestId('input-time-alias').getByRole('textbox')));

  test('Modify sql', async ({ page }) => await checkSqlEditor(page));

  test('Set legend', async ({ page }) =>
    await checkTextForm(page.getByTestId('input-metric-legend').getByRole('textbox')));

  test.describe('Visualize time series', async () => {
    test.beforeEach(async ({ page }) => {
      await page.getByLabel('toggle-viz-picker').click();
      await page.getByLabel('Plugin visualization item Time series').click();
      await page.getByTestId('select-display-type').getByText('Time Series').click();
    });

    test('Set metric alias', async ({ page }) =>
      await checkTextForm(page.getByTestId('input-metric-alias').getByRole('textbox')));

    test('Time series renders', async ({ page }) => {
      await checkTimeSeriesRenders(page);
    });
  });

  test.describe('Visualize table', async () => {
    test.beforeEach(async ({ page }) => {
      await page.getByLabel('toggle-viz-picker').click();
      await page.getByLabel('Plugin visualization item Table').click();
      await page.getByTestId('select-display-type').getByText('Table').click();
    });

    test('Table renders', async ({ page }) => {
      await checkTableRenders(page);
      await expect(page.getByLabel('Panel Title panel').getByRole('table')).toContainText('2024-07-25 08:00:00.000');
    });
  });

  test.describe('Visualize logs', async () => {
    test.beforeEach(async ({ page }) => {
      await page.getByLabel('toggle-viz-picker').click();
      await page.getByLabel('Plugin visualization item Logs').click();
      await page.getByTestId('select-display-type').getByText('Logs').click();
    });

    test('Set log alias', async ({ page }) =>
      await checkTextForm(page.getByTestId('input-log-alias').getByRole('textbox')));

    test('Logs render', async ({ page }) => {
      const dataQueryResponse = page.waitForResponse('/api/ds/query');
      const sqlPreviewResponse = page.waitForResponse('/**/resources/preview/sql/code');

      await page.getByTestId('select-table-dropdown').click();
      await page.getByText('nginxLogs', { exact: true }).click();

      await page.getByTestId('input-log-alias').getByRole('textbox').fill('message');

      const codebox = page.getByTestId('sql-editor-content').getByRole('code');

      await codebox.click();
      await page.keyboard.press('ControlOrMeta+a');
      await page.keyboard.press('ControlOrMeta+x');
      await page.keyboard.type(
        // language=text
        `SELECT "ts" as $__timeAlias(), "message", "method", "bytesSent"
FROM $__table()
WHERE $__timeFilter("ts", '12:HOURS')
ORDER BY $__timeAlias() DESC
LIMIT 100000;`
      );

      await sqlPreviewResponse;
      await expect(page.getByTestId('sql-preview')).toContainText(
        // language=text
        `SELECT "ts" as  "time" , "message", "method", "bytesSent"
FROM  "nginxLogs" 
WHERE  "ts" >= 1672531200000 AND "ts" < 1735732800000 
ORDER BY  "time"  DESC
LIMIT 100000;`
      );

      await page.getByTestId('run-query-btn').click();
      await dataQueryResponse;
      await expect(page.getByText('No data')).not.toBeVisible();
      await expect(page.getByRole('rowgroup')).toContainText(
        '143.110.222.166 - - [06/Nov/2024:21:06:58 +0000] "GET / HTTP/1.1" 403 134 "-" "Mozilla/5.0 (iPhone; CPU iPhone OS 16_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Mobile/15E148 Safari/604.1" "-"'
      );
    });
  });
});

async function checkSqlEditor(page: Page) {
  const codebox = page.getByTestId('sql-editor-content').getByRole('code');
  await codebox.click();
  await page.keyboard.press('ControlOrMeta+a');
  await page.keyboard.press('ControlOrMeta+x');
  await page.keyboard.type(
    // language=text
    `SELECT $__timeGroup("hoursSinceEpoch", '12:HOURS') AS $__timeAlias(), SUM("views") AS $__metricAlias()
FROM $__table()
WHERE $__timeFilter("hoursSinceEpoch", '12:HOURS')
GROUP BY $__timeAlias()
ORDER BY $__timeAlias() DESC
LIMIT 100000;`
  );

  await expect(codebox).toContainText(
    `123456SELECT $__timeGroup("hoursSinceEpoch", '12:HOURS') AS $__timeAlias(), SUM("views") AS $__metricAlias()FROM $__table()WHERE $__timeFilter("hoursSinceEpoch", '12:HOURS')GROUP BY $__timeAlias()ORDER BY $__timeAlias() DESCLIMIT 100000;Enter to Rename, Shift+Enter to PreviewInsert (⏎)show more (Ctrl+Space)`
  );
}

async function checkTimeSeriesRenders(page: Page) {
  const dataQueryResponse = page.waitForResponse('/api/ds/query');
  const sqlPreviewResponse = page.waitForResponse('/**/resources/preview/sql/code');

  await page.getByTestId('select-table-dropdown').click();
  await page.getByText('complex_website', { exact: true }).click();

  await page.getByTestId('input-metric-alias').getByRole('textbox').fill('views');

  const codebox = page.getByTestId('sql-editor-content').getByRole('code');
  await codebox.click();
  await page.keyboard.press('ControlOrMeta+a');
  await page.keyboard.press('ControlOrMeta+x');
  await page.keyboard.type(
    // language=text
    `SELECT $__timeGroup("hoursSinceEpoch", '12:HOURS') AS $__timeAlias(), SUM("views") AS $__metricAlias()
FROM $__table()
WHERE $__timeFilter("hoursSinceEpoch", '12:HOURS')
GROUP BY $__timeAlias()
ORDER BY $__timeAlias() DESC
LIMIT 100000;`
  );

  await sqlPreviewResponse;
  await expect(page.getByTestId('sql-preview')).toContainText(
    // language=text
    `SELECT  DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '12:HOURS')  AS  "time" , SUM("views") AS  "views" 
FROM  "complex_website" 
WHERE  "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482148 
GROUP BY  "time"
ORDER BY  "time"  DESC
LIMIT 100000;`
  );

  await page.getByTestId('run-query-btn').click();
  await dataQueryResponse;
  await expect(page.getByText('No data')).not.toBeVisible();
}

async function checkTableRenders(page: Page) {
  const dataQueryResponse = page.waitForResponse('/api/ds/query');
  const sqlPreviewResponse = page.waitForResponse('/**/resources/preview/sql/code');

  await page.getByTestId('select-table-dropdown').click();
  await page.getByText('complex_website', { exact: true }).click();

  const codebox = page.getByTestId('sql-editor-content').getByRole('code');
  await codebox.click();
  await page.keyboard.press('ControlOrMeta+a');
  await page.keyboard.press('ControlOrMeta+x');
  await page.keyboard.type(
    // language=text
    `SELECT $__timeGroup("hoursSinceEpoch", '12:HOURS') AS $__timeAlias(), SUM("views") AS "views", "country"
FROM $__table()
WHERE $__timeFilter("hoursSinceEpoch", '12:HOURS')
GROUP BY $__timeAlias(), "country"
ORDER BY $__timeAlias() DESC
LIMIT 100000;`
  );

  await sqlPreviewResponse;
  await expect(page.getByTestId('sql-preview')).toContainText(
    // language=text
    `SELECT  DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '12:HOURS')  AS  "time" , SUM("views") AS "views", "country" 
FROM  "complex_website"
WHERE  "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482148
GROUP BY  "time" , "country"
ORDER BY  "time"  DESC
LIMIT 100000;`
  );

  await page.getByTestId('run-query-btn').click();
  await dataQueryResponse;
  await expect(page.getByText('No data')).not.toBeVisible();
}
