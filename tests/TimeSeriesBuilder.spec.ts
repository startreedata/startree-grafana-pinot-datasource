import { expect, Page } from '@playwright/test';
import {
  addDashboardConstant,
  checkDropdown,
  checkFilterEditor,
  checkQueryOptionEditor,
  checkRunQueryButton,
  checkTextForm,
  queryEditorTest as test,
  selectDatasource,
  setExploreTimeWindow,
  setPanelTimeWindow,
} from '@helpers/helpers';

test('Switch between Builder and Code editor', async ({ page, datasource }) => {
  await page.goto('http://localhost:3000/dashboard/new?orgId=1');
  await page.getByLabel('Add new panel', { exact: true }).click();
  await setPanelTimeWindow(page);
  await selectDatasource(page, datasource.name);
  await page.getByTestId('select-query-type').getByText('PinotQL').click();
  await page.getByTestId('select-editor-mode').getByText('Builder').click();

  await page.getByTestId('select-table-dropdown').click();
  await page.getByLabel('Select options menu').getByText('complex_website', { exact: true }).click();
  await page.getByTestId('select-granularity-dropdown').click();
  await page.getByLabel('Select options menu').getByText('HOURS', { exact: true }).click();
  await expect(page.getByTestId('sql-preview')).toContainText(
    //language=text
    `SELECT
    DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS') AS "__time",
    SUM("views") AS "__metric"
FROM
    "complex_website"
WHERE
    "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482137
GROUP BY
    "__time"
ORDER BY
    "__time" DESC
LIMIT 100000;`
  );

  await page.getByTestId('select-editor-mode').getByText('Code').click();
  await expect(page.getByTestId('sql-preview')).toContainText(
    //language=text
    `SELECT
     DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')  AS  "__time" ,
    SUM("views") AS  "views" 
FROM
     "complex_website" 
WHERE
     "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482137 
GROUP BY
     "__time" 
ORDER BY
     "__time"  DESC
LIMIT 100000;`
  );

  await page.getByTestId('select-editor-mode').getByText('Builder').click();
  await page.getByTestId('copy-code-and-switch-btn').click();
});

test.describe('Create Panel with Time Series Builder', async () => {
  test.beforeEach(async ({ page, datasource }) => {
    const tablesResponse = page.waitForResponse('/**/resources/tables');

    await page.goto('http://localhost:3000/dashboard/new?orgId=1');
    await page.getByLabel('Add new panel', { exact: true }).click();
    await setPanelTimeWindow(page);
    await selectDatasource(page, datasource.name);
    await page.getByTestId('select-query-type').getByText('PinotQL').click();
    await page.getByTestId('select-editor-mode').getByText('Builder').click();
    await page.getByTestId('select-display-type').getByText('Time Series').click();
    await tablesResponse;
  });

  test('Run query button', async ({ page }) => await checkRunQueryButton(page));

  test('Choose table', async ({ page }) =>
    await checkDropdown(page, page.getByTestId('select-table-dropdown'), {
      want: ['complex_website', 'simple_website', 'nginxLogs'],
      setValue: 'complex_website',
    }));

  test('Choose granularity', async ({ page }) => {
    await checkGranularityDropdown(page);
  });

  test('Choose metric column', async ({ page }) => {
    await checkMetricDropdown(page);
  });

  test('Choose aggregation', async ({ page }) => {
    await checkDropdown(page, page.getByTestId('select-aggregation-dropdown'), {
      want: ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'NONE'],
      setValue: 'MAX',
    });
  });

  test('Choose group by', async ({ page }) => {
    await checkGroupByDropdown(page);
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

  test('Set series limit', async ({ page }) => {
    await page.getByTestId('input-series-limit').getByRole('textbox').fill('100');
    await expect(page.getByTestId('input-series-limit').getByRole('textbox')).toHaveValue('100');
  });

  test('Set legend', async ({ page }) => {
    await checkTextForm(page.getByTestId('input-metric-legend').getByRole('textbox'));
  });

  test('Graph renders when minimum fields are used', async ({ page }) => {
    await checkTimeSeriesRendersMinFields(page);
  });

  test('Graph renders when no aggregation is selected', async ({ page }) => {
    await checkTimeSeriesRendersNoAgg(page);
  });

  test('Graph renders when count aggregation is selected', async ({ page }) => {
    await checkTimeSeriesRendersCountAgg(page);
  });

  test('Graph renders when all fields are used', async ({ page }) => {
    await checkTimeSeriesRendersAllFields(page);
  });

  test('Leave panel and edit again', async ({ page }) => {
    await checkTimeSeriesRendersAllFields(page);
    await page.getByRole('button', { name: 'Go Back', exact: true }).click();
    await page.getByRole('heading', { name: 'Panel Title' }).click();
    await page.getByText('Edit e').click();

    await page.getByTestId('run-query-btn').click();
    await expect(page.getByTestId('sql-preview')).toContainText(
      // language=text
      `SELECT
    "browser",
    DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS') AS "__time",
    MAX("errors") AS "__metric"
FROM
    "complex_website"
WHERE
    "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482137
    AND ("country" != 'CN')
GROUP BY
    "browser",
    "__time"
ORDER BY
    "browser" ASC
LIMIT 4000;

SET timeoutMs=1000;`
    );
  });

  test('Use dashboard variables', async ({ page }) => {
    await page.getByTestId('select-table-dropdown').click();
    await page.getByText('complex_website', { exact: true }).click();

    await page.getByTestId('select-time-column-dropdown').click();
    await page.getByLabel('Select options menu').getByText('hoursSinceEpoch').click();

    await addDashboardConstant(page, 'metric', 'errors');
    await page.getByTestId('select-metric-column-dropdown').click();
    await page.keyboard.type('$metric');
    await page.keyboard.press('Enter');

    await addDashboardConstant(page, 'groupBy', 'browser');
    await page.getByTestId('select-group-by-dropdown').click();
    await page.keyboard.type('$groupBy');
    await page.keyboard.press('Enter');

    await addDashboardConstant(page, 'granularity', '1:HOURS');
    await page.getByTestId('select-granularity-dropdown').click();
    await page.keyboard.type('$granularity');
    await page.keyboard.press('Enter');

    await addDashboardConstant(page, 'aggregation', 'MAX');
    await page.getByTestId('select-aggregation-dropdown').click();
    await page.keyboard.type('$aggregation');
    await page.keyboard.press('Enter');

    await addDashboardConstant(page, 'orderBy', '__metric');
    await page.getByTestId('select-order-by-dropdown').click();
    await page.keyboard.type('$orderBy asc');
    await page.keyboard.press('Enter');

    await page.getByTestId('add-filter-btn').click();

    await addDashboardConstant(page, 'filterColumn', 'country');
    await page.getByTestId('select-query-filter-column').click();
    await page.keyboard.type('$filterColumn');
    await page.keyboard.press('Enter');

    await page.getByTestId('select-query-filter-operator').click();
    await page.getByLabel('Select options menu').getByText('!=', { exact: true }).click();

    await addDashboardConstant(page, 'filterValue', "'CN'");
    await page.getByTestId('select-query-filter-value').click();
    await page.keyboard.type('$filterValue');
    await page.keyboard.press('Enter');

    await page.getByTestId('add-query-option-btn').click();

    await addDashboardConstant(page, 'queryOptionName', 'timeoutMs');
    await page.getByTestId('select-query-option-name').click();
    await page.keyboard.type('$queryOptionName');
    await page.keyboard.press('Enter');

    await addDashboardConstant(page, 'queryOptionValue', '100');
    await page.getByTestId('input-query-option-value').getByRole('textbox').fill('$queryOptionValue');

    await page.getByTestId('run-query-btn').click();
    await expect(page.getByTestId('sql-preview')).toContainText(
      // language=text
      `SELECT
    "browser",
    DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS') AS "__time",
    MAX("errors") AS "__metric"
FROM
    "complex_website"
WHERE
    "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482137
    AND ("country" != 'CN')
GROUP BY
    "browser",
    "__time"
ORDER BY
    "__metric" ASC
LIMIT 100000;

SET timeoutMs=100;`
    );
  });
});

test.describe('Explore with Time Series Builder', async () => {
  test.beforeEach(async ({ page, datasource }) => {
    const tablesResponse = page.waitForResponse('/**/resources/tables');

    await page.goto('/explore');
    await setExploreTimeWindow(page);
    await selectDatasource(page, datasource.name);
    await page.getByTestId('select-query-type').getByText('PinotQL').click();
    await page.getByTestId('select-editor-mode').getByText('Builder').click();
    await page.getByTestId('select-display-type').getByText('Time Series').click();
    await tablesResponse;
  });

  test('Run query button', async ({ page }) => await checkRunQueryButton(page));

  test('Choose table', async ({ page }) =>
    await checkDropdown(page, page.getByTestId('select-table-dropdown'), {
      want: ['complex_website', 'simple_website', 'nginxLogs'],
      setValue: 'complex_website',
    }));

  test('Choose granularity', async ({ page }) => {
    await checkGranularityDropdown(page);
  });

  test('Choose metric column', async ({ page }) => {
    await checkMetricDropdown(page);
  });

  test('Choose aggregation', async ({ page }) => {
    await checkDropdown(page, page.getByTestId('select-aggregation-dropdown'), {
      want: ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'NONE'],
      setValue: 'MAX',
    });
  });

  test('Choose group by', async ({ page }) => {
    await checkGroupByDropdown(page);
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

  test('Set series limit', async ({ page }) => {
    await page.getByTestId('input-series-limit').getByRole('textbox').fill('100');
    await expect(page.getByTestId('input-series-limit').getByRole('textbox')).toHaveValue('100');
  });

  test('Set legend', async ({ page }) => {
    await checkTextForm(page.getByTestId('input-metric-legend').getByRole('textbox'));
  });

  test('Graph renders when minimum fields are used', async ({ page }) => {
    await checkTimeSeriesRendersMinFields(page);
  });

  test('Graph renders when no aggregation is selected', async ({ page }) => {
    await checkTimeSeriesRendersNoAgg(page);
  });

  test('Graph renders when count aggregation is selected', async ({ page }) => {
    await checkTimeSeriesRendersCountAgg(page);
  });

  test('Graph renders when all fields are used', async ({ page }) => {
    await checkTimeSeriesRendersAllFields(page);
  });
});

async function checkGranularityDropdown(page: Page) {
  const columnsResponse = page.waitForResponse('/**/resources/columns');
  const granularitiesResponse = page.waitForResponse('/**/resources/granularities');

  await page.getByTestId('select-table-dropdown').click();
  await page.getByLabel('Select options menu').getByText('complex_website', { exact: true }).click();

  await columnsResponse;
  await page.getByTestId('select-time-column-dropdown').click();
  await page.getByLabel('Select options menu').getByText('hoursSinceEpoch', { exact: true }).click();

  await granularitiesResponse;
  const dropdownLocator = page.getByTestId('select-granularity-dropdown');
  await checkDropdown(page, dropdownLocator, {
    want: ['auto', 'HOURS', 'DAYS'],
    setValue: 'HOURS',
  });
}

async function checkMetricDropdown(page: Page) {
  const columnsResponse = page.waitForResponse('/**/resources/columns');

  await page.getByTestId('select-table-dropdown').click();
  await page.getByLabel('Select options menu').getByText('complex_website', { exact: true }).click();

  await columnsResponse;

  const dropdownLocator = page.getByTestId('select-metric-column-dropdown');
  await checkDropdown(page, dropdownLocator, {
    want: ['clicks', 'errors', 'views'],
    setValue: 'errors',
  });
}

async function checkGroupByDropdown(page: Page) {
  const columnsResponse = page.waitForResponse('/**/resources/columns');

  await page.getByTestId('select-table-dropdown').click();
  await page.getByLabel('Select options menu').getByText('complex_website', { exact: true }).click();

  await columnsResponse;
  await page.getByTestId('select-metric-column-dropdown').click();
  await page.getByLabel('Select options menu').getByText('views', { exact: true }).click();

  const dropdownLocator = page.getByTestId('select-group-by-dropdown');
  await checkDropdown(page, dropdownLocator, {
    want: ['country', 'browser', 'platform', 'clicks', 'errors'],
    dontWant: ['views'],
    setValue: 'country',
  });
}

async function checkOrderByDropdown(page: Page) {
  const columnsResponse = page.waitForResponse('/**/resources/columns');

  await page.getByTestId('select-table-dropdown').click();
  await page.getByLabel('Select options menu').getByText('complex_website', { exact: true }).click();

  await columnsResponse;
  await page.getByTestId('select-metric-column-dropdown').click();
  await page.getByLabel('Select options menu').getByText('views', { exact: true }).click();
  await page.getByTestId('select-group-by-dropdown').click();
  await page.getByLabel('Select options menu').getByText('country', { exact: true }).click();

  const dropdownLocator = page.getByTestId('select-order-by-dropdown');
  await checkDropdown(page, dropdownLocator, {
    want: ['__time asc', '__time desc', '__metric asc', '__metric desc', 'country asc', 'country desc'],
    setValue: '__metric asc',
  });
}

async function checkTimeSeriesRendersMinFields(page: Page) {
  await page.getByTestId('select-table-dropdown').click();
  await page.getByLabel('Select options menu').getByText('complex_website', { exact: true }).click();

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
LIMIT 100000;`
  );

  await expect(page.getByText('No data')).not.toBeVisible();
}

async function checkTimeSeriesRendersNoAgg(page: Page) {
  await page.getByTestId('select-table-dropdown').click();
  await page.getByLabel('Select options menu').getByText('complex_website', { exact: true }).click();

  await page.getByTestId('select-aggregation-dropdown').click();
  await page.getByLabel('Select options menu').getByText('NONE', { exact: true }).click();

  await expect(page.getByTestId('sql-preview')).toContainText(
    // language=text
    `SELECT
    "views" AS "__metric",
    "hoursSinceEpoch" AS "__time"
FROM
    "complex_website"
WHERE
    "views" IS NOT NULL
    AND "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482136
ORDER BY "__time" DESC
LIMIT 100000;`
  );

  await expect(page.getByText('No data')).not.toBeVisible();
}

async function checkTimeSeriesRendersCountAgg(page: Page) {
  await page.getByTestId('select-table-dropdown').click();
  await page.getByLabel('Select options menu').getByText('complex_website', { exact: true }).click();

  await page.getByTestId('select-aggregation-dropdown').click();
  await page.getByLabel('Select options menu').getByText('COUNT', { exact: true }).click();

  await expect(page.getByTestId('sql-preview')).toContainText(
    // language=text
    `SELECT
    DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '12:HOURS') AS "__time",
    COUNT("*") AS "__metric"
FROM
    "complex_website"
WHERE
    "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482148
GROUP BY
    "__time"
ORDER BY
    "__time" DESC
LIMIT 100000;`
  );

  await expect(page.getByText('No data')).not.toBeVisible();
}

async function checkTimeSeriesRendersAllFields(page: Page) {
  await page.getByTestId('select-table-dropdown').click();
  await page.getByText('complex_website', { exact: true }).click();

  await page.getByTestId('select-time-column-dropdown').click();
  await page.getByLabel('Select options menu').getByText('hoursSinceEpoch').click();
  await page.getByTestId('select-metric-column-dropdown').click();
  await page.getByLabel('Select options menu').getByText('errors', { exact: true }).click();
  await page.getByTestId('select-group-by-dropdown').click();
  await page.getByLabel('Select options menu').getByText('browser', { exact: true }).click();
  await page.getByTestId('select-granularity-dropdown').click();
  await page.getByLabel('Select options menu').getByText('HOURS', { exact: true }).click();
  await page.getByTestId('select-aggregation-dropdown').click();
  await page.getByLabel('Select options menu').getByText('MAX', { exact: true }).click();
  await page.getByTestId('select-order-by-dropdown').click();
  await page.getByLabel('Select options menu').getByText('browser asc', { exact: true }).click();

  await page.getByTestId('add-filter-btn').click();
  await page.getByTestId('select-query-filter-column').click();
  await page.getByLabel('Select options menu').getByText('country', { exact: true }).click();
  await page.getByTestId('select-query-filter-operator').click();
  await page.getByLabel('Select options menu').getByText('!=', { exact: true }).click();
  await page.getByTestId('select-query-filter-value').click();
  await page.getByLabel('Select options menu').getByText(`'CN'`, { exact: true }).click();
  await page.locator('body').click();

  await page.getByTestId('add-query-option-btn').click();
  await page.getByTestId('select-query-option-name').click();
  await page.getByLabel('Select options menu').getByText('timeoutMs', { exact: true }).click();
  await page.getByTestId('input-query-option-value').getByRole('textbox').fill('1000');

  await page.getByTestId('input-limit').getByRole('textbox').fill('4000');
  await page.getByTestId('input-series-limit').getByRole('textbox').fill('2');
  await page.getByTestId('input-metric-legend').getByRole('textbox').fill('{{browser}}');

  await page.getByTestId('run-query-btn').click();

  await expect(page.getByTestId('sql-preview')).toContainText(
    // language=text
    `SELECT
    "browser",
    DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS') AS "__time",
    MAX("errors") AS "__metric"
FROM
    "complex_website"
WHERE
    "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482137
    AND ("country" != 'CN')
GROUP BY
    "browser",
    "__time"
ORDER BY
    "browser" ASC
LIMIT 4000;

SET timeoutMs=1000;`
  );

  await expect(page.getByText('No data')).not.toBeVisible();
  await expect(page.getByLabel('VizLegend series chrome')).toBeVisible();
  await expect(page.getByLabel('VizLegend series edge')).toBeVisible();
  await expect(page.getByText('chromeedge', { exact: true })).toBeVisible();
}
