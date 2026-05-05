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

export async function setDashboardTimeWindow(page: Page) {
  await setExploreTimeWindow(page);
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
  // Two elements match this text (the dropdown list item and the closed-state container);
  // .first() picks the visible option in the dropdown to satisfy Playwright's strict mode.
  await page.getByText('Coordinated Universal TimeUTC').first().click();
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

/**
 * Adds a Pinot Query template variable to the current dashboard.
 *
 * Opens dashboard settings → Variables → New variable, sets Type=Query, picks
 * the datasource, switches to the "Sql Query" tab, pastes the SQL into the
 * Monaco editor, optionally toggles Multi-value / Include All, submits, and
 * closes the settings dialog.
 *
 * Caller must have already navigated to a dashboard.
 */
export async function addPinotQueryVariable(
  page: Page,
  datasourceName: string,
  opts: { name: string; sql: string; multi?: boolean; includeAll?: boolean; table?: string }
) {
  // After a previous variable was created via this helper we may still be on the
  // Variables list with the settings dialog open (Submit + Go Back returns here).
  // Only open dashboard settings + click the Variables link if we're not already
  // showing the variables list — otherwise we'd toggle the dialog off and lose
  // access to the Variables link inside it.
  const onVariablesList = await page.getByRole('button', { name: 'Variable editor New variable' }).isVisible().catch(() => false);
  if (!onVariablesList) {
    // Use the sidebar's "Dashboard settings" link rather than the toolbar cog button —
    // on a fresh `/dashboard/new` page, Grafana's Add Panel modal occludes the toolbar.
    await page.getByLabel('Dashboard settings').click();
    await page.getByRole('link', { name: 'Variables' }).click();
  }

  if (await page.getByTestId('data-testid Call to action button Add variable').isVisible().catch(() => false)) {
    await page.getByTestId('data-testid Call to action button Add variable').click();
  } else {
    await page.getByRole('button', { name: 'Variable editor New variable' }).click();
  }

  // Type=Query is the default for new variables, so we don't need to set it.
  // Set name.
  await page.getByTestId('data-testid Variable editor Form Name field').fill(opts.name);

  // Pick the datasource — this triggers a /tables fetch.
  const tablesResponse = page.waitForResponse('/**/resources/tables');
  await page.getByLabel('Data source picker select').click();
  await page.getByText(datasourceName).click();
  await tablesResponse;

  // Switch to "Sql Query" tab. The four type-tabs ('Tables', 'Columns', 'Distinct Values', 'Sql Query')
  // appear after datasource selection. Use exact: true so we don't match other "Sql Query"-containing text.
  // The table dropdown is only rendered after switching off the default 'Tables' type, so it must
  // be picked AFTER the tab switch.
  await expect(page.getByText('Sql Query', { exact: true })).toBeVisible();
  await page.getByText('Sql Query', { exact: true }).click();

  // Optionally pick the table the variable's SQL targets — required when the SQL uses
  // `$__table()` macro (otherwise it expands to nothing and the variable returns empty).
  if (opts.table) {
    await page.getByTestId('select-table-dropdown').click();
    await page.getByLabel('Select options menu').getByText(opts.table, { exact: true }).click();
  }
  const dataQueryResponse = page.waitForResponse('/api/ds/query');
  const codebox = page.getByTestId('sql-editor-content').getByRole('code');
  await codebox.click();
  await page.keyboard.press('ControlOrMeta+a');
  await page.keyboard.press('Delete');
  await page.keyboard.type(opts.sql);
  await dataQueryResponse;

  // Wait for "Preview of values" to populate (no longer "None") — without this guard,
  // submitting before the variable's options arrive results in an empty options list, and
  // any later "All" selection on the dashboard top bar yields nothing.
  await expect(page.getByText('None', { exact: true })).not.toBeVisible({ timeout: 10000 });

  // Toggle Multi-value if requested (default off in Grafana). Clicking the parent div
  // containing the "Multi-value" label is more reliable than the hidden <input type=checkbox>.
  if (opts.multi) {
    await page.locator('div').filter({ hasText: /^Multi-value$/ }).nth(2).click();
  }
  // Toggle Include All option if requested.
  if (opts.includeAll) {
    await page.locator('div').filter({ hasText: /^Include All option$/ }).nth(2).click();
  }

  await page.getByRole('button', { name: 'Variable editor Submit button' }).click();
  await page.getByRole('dialog', { name: 'Dashboard settings' }).getByLabel('Go Back').click();
}

/**
 * Adds a panel with PinotQL **Code editor** (raw SQL) instead of the structured Builder UI.
 * Picks the datasource, sets editor mode to Code, picks the table (used for `$__table()` macro
 * expansion), then clears the Monaco editor and types the user-supplied SQL into it.
 *
 * Caller must have already navigated to a dashboard.
 */
export async function addPanelInCodeMode(
  page: Page,
  datasourceName: string,
  opts: { table: string; code: string }
) {
  await page.getByLabel('Add new panel', { exact: true }).click();
  // Arm the /tables wait BEFORE selecting the datasource — Grafana fires the request once
  // during datasource selection, so setting up the wait afterwards races and times out in CI.
  const tablesResponse = page.waitForResponse('/**/resources/tables');
  await selectDatasource(page, datasourceName);
  await tablesResponse;

  await page.getByTestId('select-query-type').getByText('PinotQL').click();
  await page.getByTestId('select-editor-mode').getByText('Code').click();

  await page.getByTestId('select-table-dropdown').click();
  await page.getByLabel('Select options menu').getByText(opts.table, { exact: true }).click();

  // Replace any starter content in the Monaco editor with the user-supplied SQL.
  const codebox = page.getByTestId('sql-editor-content').getByRole('code');
  await codebox.click();
  await page.keyboard.press('ControlOrMeta+a');
  await page.keyboard.press('Delete');
  await page.keyboard.type(opts.code);

  // Force-click run-query (panel-options sidebar can have a loading overlay).
  await page.getByTestId('run-query-btn').click({ force: true });
}

export async function addDashboardConstant(page: Page, name: string, value: string) {
  await page.getByRole('button', { name: 'Open dashboard settings' }).click();
  await page.getByRole('link', { name: 'Variables' }).click();

  if (await page.getByTestId('data-testid Call to action button Add variable').isVisible()) {
    await page.getByTestId('data-testid Call to action button Add variable').click();
  } else {
    await page.getByRole('button', { name: 'Variable editor New variable' }).click();
  }

  await page.getByTestId('data-testid Variable editor Form Type select').getByRole('img').click();
  await page.getByText('Constant', { exact: true }).click();
  await page.getByTestId('data-testid Variable editor Form Name field').click();
  await page.getByTestId('data-testid Variable editor Form Name field').fill(name);
  await page.getByTestId('data-testid Variable editor Form Constant Query field').click();
  await page.getByTestId('data-testid Variable editor Form Constant Query field').fill(value);
  await page.getByRole('button', { name: 'Variable editor Submit button' }).click();
  await page.getByRole('dialog', { name: 'Dashboard settings' }).getByLabel('Go Back').click();
}
