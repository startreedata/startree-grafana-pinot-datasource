import { expect, Locator, Page, test as base } from '@playwright/test';

export const Env = Object.freeze({
  PinotConnectionControllerUrl: process.env['PINOT_CONNECTION_CONTROLLER_URL'],
  PinotConnectionBrokerUrl: process.env['PINOT_CONNECTION_BROKER_URL'],
  PinotConnectionDatabase: process.env['PINOT_CONNECTION_DATABASE'],
  PinotConnectionAuthToken: process.env['PINOT_CONNECTION_AUTH_TOKEN'],
});

interface QueryEditorFixtures {
  datasource: { uid: string; name: string };
}

export const queryEditorTest = base.extend<QueryEditorFixtures>({
  datasource: async ({}, use) => {
    const datasource = await createDatasource();
    await use(datasource);
    await deleteDatasource(datasource.uid);
  },
});

export async function deleteDatasource(uid: string): Promise<void> {
  return fetch(`http://localhost:3000/api/datasources/uid/${uid}`, {
    method: 'DELETE',
  }).then((response) => {
    if (response.status === 200) {
      return;
    } else {
      console.error(response);
      throw new Error(response.statusText);
    }
  });
}

export function randomDatasourceName(): string {
  return `__Pinot_Test_${Math.floor(Math.random() * 1e6).toString(36)}`;
}

export async function createDatasource(): Promise<{ uid: string; name: string }> {
  return fetch('http://localhost:3000/api/datasources/', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name: randomDatasourceName(),
      type: 'startree-pinot-datasource',
      typeName: 'Pinot',
      typeLogoUrl: 'public/plugins/startree-pinot-datasource/img/logo.svg',
      jsonData: {
        brokerUrl: Env.PinotConnectionBrokerUrl,
        controllerUrl: Env.PinotConnectionControllerUrl,
        databaseName: Env.PinotConnectionDatabase,
        tokenType: 'Bearer',
      },
      secureJsonData: {
        authToken: Env.PinotConnectionAuthToken,
      },
      access: 'proxy',
    }),
  })
    .then((response) => {
      if (response.status === 200) {
        return response.json();
      } else {
        console.error(response);
        throw new Error(response.statusText);
      }
    })
    .then((data: { datasource: { uid: string; name: string } }) => ({
      name: data.datasource.name,
      uid: data.datasource.uid,
    }));
}

export async function selectDatasource(page: Page, name: string) {
  await page.getByLabel('Data source picker select').click();
  await page.getByText(new RegExp(`^${name}`)).click();
}

export async function setExploreTimeWindow(page: Page) {
  await page.getByTestId('data-testid TimePicker Open Button').click();
  await setTimeWindow(page);
}

export async function setPanelTimeWindow(page: Page) {
  await page.getByLabel('Panel editor content').getByTestId('data-testid TimePicker Open Button').click();
  await setTimeWindow(page);
}

async function setTimeWindow(page: Page) {
  await page.getByRole('button', { name: 'Change time settings' }).click();
  await page
    .getByTestId('data-testid Time zone picker select container')
    .locator('div')
    .filter({ hasText: 'Type to search (country, city' })
    .nth(1)
    .click();
  await page.getByText('Coordinated Universal TimeUTC').click();
  await page.getByLabel('Time Range from field').click();
  await page.getByLabel('Time Range from field').fill('2023-01-01');
  await page.getByLabel('Time Range to field').click();
  await page.getByLabel('Time Range to field').fill('2025-01-01');
  await page.getByTestId('data-testid TimePicker submit button').click();
}

export async function checkDropdown(
  page: Page,
  dropdown: Locator,
  params: {
    want?: string[];
    dontWant?: string[];
    onOpen?: () => void;
    setValue?: string;
  }
): Promise<void> {
  await dropdown.click();

  if (params.onOpen) {
    params.onOpen();
  }

  const optionsMenu = page.getByLabel('Select options menu');
  for (const text of params.want || []) {
    await expect(optionsMenu.getByText(text, { exact: true }), text).toBeVisible();
  }

  for (const text of params.dontWant || []) {
    await expect(optionsMenu.getByText(text, { exact: true }), text).not.toBeVisible();
  }

  if (params.setValue) {
    await optionsMenu.getByText(params.setValue, { exact: true }).click();
    await expect(dropdown).toContainText(params.setValue);
  }

  await page.locator('body').click();
}

export async function checkTextForm(textbox: Locator) {
  await textbox.fill('my_input_data');
  await expect(textbox).toHaveValue('my_input_data');
  await textbox.clear();
  await expect(textbox).toHaveValue('');
}

export async function checkRunQueryButton(page: Page) {
  const dataQueryResponse = page.waitForResponse('/api/ds/query');
  await page.getByTestId('run-query-btn').click();
  await dataQueryResponse;
}

export async function checkFilterEditor(page: Page) {
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

export async function checkQueryOptionEditor(page: Page) {
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
