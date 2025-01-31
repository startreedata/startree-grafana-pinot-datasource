import { queryEditorTest as test, setDashboardTimeWindow } from '@helpers/helpers';
import { expect } from '@playwright/test';

test('Annotations Query Editor', async ({ page, datasource }) => {
  await page.goto('http://localhost:3000/dashboard/new?orgId=1');
  await setDashboardTimeWindow(page);

  await page.getByRole('button', { name: 'Dashboard settings' }).click();
  await page.getByRole('link', { name: 'Annotations' }).click();
  await page.getByTestId('data-testid Call to action button Add annotation query').click();

  await page.getByLabel('Data source picker select').locator('svg').click();
  await page.getByText(datasource.name).click();

  const dataQueryResponse = page.waitForResponse('/api/ds/query');
  const sqlPreviewResponse = page.waitForResponse('/**/resources/preview/sql/code');

  await page.getByTestId('select-table-dropdown').locator('div').filter({ hasText: 'Choose' }).nth(1).click();
  await page.getByText('complex_website', { exact: true }).click();

  const codebox = page.getByTestId('sql-editor-content').getByRole('code');
  await codebox.click();
  await page.keyboard.press('ControlOrMeta+a');
  await page.keyboard.press('ControlOrMeta+x');
  await page.keyboard.type(
    // language=text
    `SELECT $__timeGroup("hoursSinceEpoch", '12:HOURS') AS "time", SUM("views") AS "views", "country"
FROM $__table()
WHERE $__timeFilter("hoursSinceEpoch", '12:HOURS')
GROUP BY "time", "country"
ORDER BY "time" DESC
LIMIT 100000;`
  );
  await page.getByTestId('run-query-btn').click();

  await sqlPreviewResponse;
  await expect(page.getByTestId('sql-preview')).toContainText(
    // language=text
    `SELECT  DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '12:HOURS')  AS "time", SUM("views") AS "views", "country"
FROM  "complex_website" 
WHERE  "hoursSinceEpoch" >= 464592 AND "hoursSinceEpoch" < 482148 
GROUP BY "time", "country"
ORDER BY "time" DESC
LIMIT 100000;`
  );

  await expect(page.getByText('336 events (from 3 fields)')).toBeVisible();
});
