import { expect, Locator, Page } from '@playwright/test';

export const Env = Object.freeze({
  // PinotConnectionControllerUrl: process.env['pinotConnectionControllerUrl'],
  // PinotConnectionBrokerUrl: process.env['pinotConnectionBrokerUrl'],
  // PinotConnectionDatabase: process.env['pinotConnectionDatabase'],
  // PinotConnectionAuthToken: process.env['pinotConnectionAuthToken'],
  PinotConnectionControllerUrl: 'https://pinot.celpxu.cp.s7e.startree.cloud',
  PinotConnectionBrokerUrl: 'https://broker.pinot.celpxu.cp.s7e.startree.cloud',
  PinotConnectionDatabase: 'ws_2jkxph6tf0nr',
  PinotConnectionAuthToken: 'st-JzYjKgW5vpcUsEm9-Z3JRnh4uXRME90HIAMCuoySPPjgCXgdI',
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

export async function createDatasource(): Promise<{ uid: string; name: string }> {
  return fetch('http://localhost:3000/api/datasources/', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name: `__Pinot_Test_${Math.floor(Math.random() * 1e6).toString(36)}`,
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
