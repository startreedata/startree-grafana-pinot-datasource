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

test('Switch between Builder and Code editor', async ({ page, datasource }) => {
  const tablesResponse = page.waitForResponse('/**/resources/tables');
  const builderPreviewResponse = page.waitForResponse('/**/resources/preview/sql/builder');
  const codePreviewResponse = page.waitForResponse('/**/resources/preview/sql/code');

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
  await builderPreviewResponse;
  await expect(page.getByTestId('sql-preview')).toContainText(
    //language=text
    `SELECT
    DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS') AS "time",
    SUM("views") AS "metric"
FROM
    "complex_website"
WHERE
    "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482137
GROUP BY
    "time"
ORDER BY
    "time" DESC
LIMIT 100000;`
  );

  await page.getByTestId('select-editor-mode').getByText('Code').click();
  await codePreviewResponse;
  await expect(page.getByTestId('sql-preview')).toContainText(
    //language=text
    `SELECT
     DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')  AS  "time" ,
    SUM("views") AS  "views" 
FROM
     "complex_website" 
WHERE
     "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482137 
GROUP BY
     "time" 
ORDER BY
     "time"  DESC
LIMIT 100000;`
  );

  await page.getByTestId('select-editor-mode').getByText('Builder').click();
  await page.getByTestId('copy-code-and-switch-btn').click();
});

test.describe('Create Panel with Builder', async () => {
  test.beforeEach(async ({ page, datasource }) => {
    const tablesResponse = page.waitForResponse('/**/resources/tables');

    await page.goto('http://localhost:3000/dashboard/new?orgId=1');
    await page.getByLabel('Add new panel', { exact: true }).click();
    await setPanelTimeWindow(page);
    await selectDatasource(page, datasource.name);
    await page.getByTestId('select-query-type').getByText('PinotQL').click();
    await page.getByTestId('select-editor-mode').getByText('Builder').click();
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

  test('Set legend', async ({ page }) => {
    await checkTextForm(page.getByTestId('input-metric-legend').getByRole('textbox'));
  });

  test('Graph renders when minimum fields are used', async ({ page }) => {
    await checkTimeSeriesRendersMinFields(page);
  });

  test('Graph renders when all fields are used', async ({ page }) => {
    await checkTimeSeriesRendersAllFields(page);
  });
});

test.describe('Explore with Builder', async () => {
  test.beforeEach(async ({ page, datasource }) => {
    const tablesResponse = page.waitForResponse('/**/resources/tables');

    await page.goto('/explore');
    await setExploreTimeWindow(page);
    await selectDatasource(page, datasource.name);
    await page.getByTestId('select-query-type').getByText('PinotQL').click();
    await page.getByTestId('select-editor-mode').getByText('Builder').click();
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

  test('Set legend', async ({ page }) => {
    await checkTextForm(page.getByTestId('input-metric-legend').getByRole('textbox'));
  });

  test('Graph renders when minimum fields are used', async ({ page }) => {
    await checkTimeSeriesRendersMinFields(page);
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
    want: ['time asc', 'time desc', 'metric asc', 'metric desc', 'country asc', 'country desc'],
    setValue: 'metric asc',
  });
}

async function checkFilterEditor(page: Page) {
  const columnsResponse = page.waitForResponse('/**/resources/columns');
  const distinctValuesResponse = page.waitForResponse('/**/resources/query/distinctValues');

  await page.getByTestId('select-table-dropdown').click();
  await page.getByLabel('Select options menu').getByText('complex_website', { exact: true }).click();

  await columnsResponse;
  await page.getByTestId('select-time-column-dropdown').click();
  await page.getByLabel('Select options menu').getByText('hoursSinceEpoch', { exact: true }).click();

  await page.getByTestId('add-filter-btn').click();
  await checkDropdown(page, page.getByTestId('select-query-filter-column'), {
    want: ['country', 'browser', 'platform', 'clicks', 'views', 'errors'],
  });
  await page.getByTestId('select-query-filter-column').click();
  await page.getByLabel('Select options menu').getByText('country', { exact: true }).click();
  await expect(page.getByTestId('select-query-filter-column')).toContainText('country');

  await checkDropdown(page, page.getByTestId('select-query-filter-operator'), {
    want: ['=', '!=', '>', '<', '<=', '<=', 'like', 'not like'],
  });
  await page.getByTestId('select-query-filter-operator').click();
  await page.getByLabel('Select options menu').getByText('=', { exact: true }).click();
  await expect(page.getByTestId('select-query-filter-operator')).toContainText('=');

  await checkDropdown(page, page.getByTestId('select-query-filter-value'), {
    onOpen: async () => await distinctValuesResponse,
    want: [`'CN'`, `'IN'`, `'KR'`, `'US'`],
  });
  await page.getByTestId('select-query-filter-value').click();
  await page.getByLabel('Select options menu').getByText(`'IN'`, { exact: true }).click();
  await expect(page.getByTestId('select-query-filter-value')).toContainText(`'IN'`);

  await page.getByTestId('delete-filter-btn').click();
  await expect(page.getByTestId('select-query-filter-column')).not.toBeVisible();
}

async function checkQueryOptionEditor(page: Page) {
  await page.getByTestId('add-query-option-btn').click();

  await checkDropdown(page, page.getByTestId('select-query-option-name'), {
    want: [
      'timeoutMs',
      'enableNullHandling',
      'explainPlanVerbose',
      'useMultistageEngine',
      'maxExecutionThreads',
      'numReplicaGroupsToQuery',
      'minSegmentGroupTrimSize',
      'minServerGroupTrimSize',
      'skipIndexes',
      'skipUpsert',
      'useStarTree',
      'maxRowsInJoin',
      'inPredicatePreSorted',
      'inPredicateLookupAlgorithm',
      'maxServerResponseSizeBytes',
      'maxQueryResponseSizeBytes',
    ],
  });
  await page.getByTestId('select-query-option-name').click();
  await page.getByLabel('Select options menu').getByText('timeoutMs', { exact: true }).click();
  await expect(page.getByTestId('select-query-option-name')).toContainText('timeoutMs');

  await page.getByTestId('input-query-option-value').getByRole('textbox').fill('100');
  await expect(page.getByTestId('input-query-option-value').getByRole('textbox')).toHaveValue('100');

  await page.getByTestId('delete-query-option-btn').click();
  await expect(page.getByTestId('select-query-option-name')).not.toBeVisible();
}

async function checkTimeSeriesRendersMinFields(page: Page) {
  const queryResponse = page.waitForResponse('/api/ds/query');
  const sqlPreviewResponse = page.waitForResponse('/**/resources/preview/sql/builder');

  await page.getByTestId('select-table-dropdown').click();
  await page.getByLabel('Select options menu').getByText('complex_website', { exact: true }).click();

  await sqlPreviewResponse;
  await expect(page.getByTestId('sql-preview')).toContainText(
    // language=text
    `SELECT
    DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '12:HOURS') AS "time",
    SUM("views") AS "metric"
FROM
    "complex_website"
WHERE
    "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482148
GROUP BY
    "time"
ORDER BY
    "time" DESC
LIMIT 100000;`
  );

  await queryResponse;
  await expect(page.getByText('No data')).not.toBeVisible();
}

async function checkTimeSeriesRendersAllFields(page: Page) {
  const columnsResponse = page.waitForResponse('/**/resources/columns');
  const distinctValuesResponse = page.waitForResponse('/**/resources/query/distinctValues');
  const sqlPreviewResponse = page.waitForResponse('/**/resources/preview/sql/builder');
  const dataQueryResponse = page.waitForResponse('/api/ds/query');

  await page.getByTestId('select-table-dropdown').click();
  await page.getByText('complex_website', { exact: true }).click();
  await columnsResponse;

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
  await distinctValuesResponse;
  await page.getByLabel('Select options menu').getByText(`'CN'`, { exact: true }).click();
  await page.getByText("'CN'", { exact: true }).click();
  await page.locator('body').click();

  await page.getByTestId('add-query-option-btn').click();
  await page.getByTestId('select-query-option-name').click();
  await page.getByLabel('Select options menu').getByText('timeoutMs', { exact: true }).click();
  await page.getByTestId('input-query-option-value').getByRole('textbox').fill('100');
  await page.getByTestId('input-limit').getByRole('textbox').fill('4000');
  await page.getByTestId('input-metric-legend').getByRole('textbox').fill('{{browser}}');

  await sqlPreviewResponse;
  await expect(page.getByTestId('sql-preview')).toContainText(
    // language=text
    `SET timeoutMs=100;

SELECT
    "browser",
    DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS') AS "time",
    MAX("errors") AS "metric"
FROM
    "complex_website"
WHERE
    "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482137
    AND ("country" != 'CN')
GROUP BY
    "browser",
    "time"
ORDER BY
    "browser" ASC
LIMIT 4000;`
  );

  await page.getByTestId('run-query-btn').click();
  await dataQueryResponse;
  await expect(page.getByText('No data')).not.toBeVisible();
}
