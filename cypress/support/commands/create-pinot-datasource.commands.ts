import { EnvVariables } from '../constants/env-variables';

Cypress.Commands.add('createPinotDatasource', () => {
  const params = {
    controllerUrl: Cypress.env(EnvVariables.pinotConnectionControllerUrl),
    brokerUrl: Cypress.env(EnvVariables.pinotConnectionBrokerUrl),
    databaseName: Cypress.env(EnvVariables.pinotConnectionDatabase),
    authType: 'Bearer',
    authToken: Cypress.env(EnvVariables.pinotConnectionAuthToken),
  };

  const name = `__Pinot_Test_${Math.floor(Math.random() * 1e6).toString(36)}`;
  return cy
    .request({
      url: 'http://localhost:3000/api/datasources',
      method: 'POST',
      headers: { ContentType: 'application/json' },
      body: {
        name: name,
        type: 'startree-pinot-datasource',
        typeName: 'Pinot',
        typeLogoUrl: 'public/plugins/startree-pinot-datasource/img/logo.svg',
        jsonData: {
          brokerUrl: params.brokerUrl,
          controllerUrl: params.controllerUrl,
          databaseName: params.databaseName,
          tokenType: params.authType,
        },
        secureJsonData: {
          authToken: params.authToken,
        },
        access: 'proxy',
      },
    })
    .then((response) => {
      if (response.status === 200) {
        return response.body as { datasource: { uid: string; name: string } };
      } else {
        throw new Error(response.statusText);
      }
    })
    .then((data) => ({
      name: data.datasource.name,
      uid: data.datasource.uid,
    }));
});

Cypress.Commands.add('selectDatasource', (name: string) => {
  cy.get('#data-source-picker').should('be.visible');
  cy.get('#data-source-picker').parent().parent().as('dataSourcePicker');
  cy.get('@dataSourcePicker').click();

  cy.get('[aria-label="Select options menu"]').should('be.visible');
  cy.get('[aria-label="Select options menu"]').within(() => {
    cy.contains(name).click();
  });

  cy.get('@dataSourcePicker').should('contain.text', name);
});

Cypress.Commands.add('setDashboardTimeRange', (timeRange: { from: string; to: string }) => {
  cy.get('[data-testid="data-testid TimePicker Open Button"]').should('exist').as('timePickerButton');
  cy.get('@timePickerButton').click();

  cy.get('#TimePickerContent').should('be.visible').as('timePickerContent');
  cy.get('@timePickerContent').within(() => {
    cy.get('input[aria-label="Time Range from field"]').should('exist').as('timeFromField');
    cy.get('@timeFromField').clear();
    cy.get('@timeFromField').type(timeRange.from);

    cy.get('input[aria-label="Time Range to field"]').should('exist').as('timeToField');
    cy.get('@timeToField').clear();
    cy.get('@timeToField').type(timeRange.to);

    cy.get('button').contains('Apply time range').click();
  });
});

Cypress.Commands.add('pinotQlBuilder_CheckSqlPreview', (want?: string) => {
  cy.getBySel('sql-preview-container').should('exist');
  cy.getBySel('sql-preview-container').within(() => {
    cy.checkFormLabel({ wantLabel: 'Sql Preview' });
    cy.getBySel('sql-preview').should('exist').and('not.be.empty');
    if (want !== undefined) {
      cy.getBySel('sql-preview').should('have.text', want);
    }
  });
});

Cypress.Commands.add('checkFormLabel', (params: { wantLabel: string }) => {
  const { wantLabel } = params;
  cy.get(`[data-testid=inline-form-label]`).should('exist').and('have.text', wantLabel);
});

Cypress.Commands.add(
  'checkTextField',
  (params: { testId: string; wantLabel?: string; wantContent?: string; wantPlaceholder?: string }) => {
    const { testId, wantLabel, wantContent, wantPlaceholder } = params;
    cy.wrap(testId).should('not.be.empty');

    cy.get(`[data-testid=${testId}]`).should('exist');
    cy.get(`[data-testid=${testId}]`).within(() => {
      if (wantLabel !== undefined) {
        cy.checkFormLabel({ wantLabel });
      }
      if (wantContent !== undefined) {
        cy.get('input').should('exist').and('have.value', wantContent);
      }
      if (wantPlaceholder !== undefined) {
        cy.get('input').should('have.attr', 'placeholder', wantPlaceholder);
      }
    });
  }
);

Cypress.Commands.add('fillTextField', (params: { testId: string; content: string }) => {
  const { testId, content } = params;
  cy.wrap(testId).should('not.be.empty');

  cy.get(`[data-testid=${testId}]`).should('exist');
  cy.get(`[data-testid=${testId}]`).within(() => {
    cy.get('input').should('exist');
    cy.get('input').clear();
    cy.get('input').type(content);
    cy.get('input').should('exist').and('have.value', content);
  });
});

Cypress.Commands.add('selectFromDropdown', (params: { testId: string; value: string }) => {
  const { testId, value } = params;
  cy.wrap(testId).should('not.be.empty');

  cy.get(`[data-testid=${testId}]`).should('exist');
  cy.get(`[data-testid=${testId}]`).within(() => {
    cy.get('input').parent().parent().as('dropdownSelect');
    cy.get('@dropdownSelect').click();

    cy.wrap(cy.$$('body')).as('body');
    cy.get('@body').find('[aria-label="Select options menu"]').should('be.visible');

    cy.get('@body')
      .find('[aria-label="Select options menu"]')
      .within(() => cy.contains(value).click());

    cy.get('@dropdownSelect').within(() => {
      cy.contains(value);
    });
  });
});

Cypress.Commands.add(
  'checkDropdown',
  (params: { testId: string; wantLabel?: string; wantSelected?: string; wantOptions?: string[] }) => {
    const { testId, wantLabel, wantSelected, wantOptions } = params;
    cy.wrap(testId).should('not.be.empty');

    cy.get(`[data-testid=${testId}]`).should('exist');
    cy.get(`[data-testid=${testId}]`).within(() => {
      if (wantLabel !== undefined) {
        cy.checkFormLabel({ wantLabel });
      }

      const dropdownSelect = cy.get('input').parent().parent();
      dropdownSelect.within(() => {
        if (wantSelected) {
          cy.contains(wantSelected);
        }
      });

      dropdownSelect.click();
      cy.wrap(cy.$$('body')).as('body');
      cy.get('@body').find('[aria-label="Select options menu"]').should('be.visible');
      cy.get('@body')
        .find('[aria-label="Select options menu"]')
        .within(() => {
          wantOptions?.forEach((option) => cy.contains(option));
        });
    });
    cy.root().click(0, 0, { force: true });
  }
);

Cypress.Commands.add('pinotQlBuilder_AddQueryOptions', (params: { name: string; value: string }) => {
  const { name, value } = params;

  cy.getBySel('select-query-options').should('exist');
  cy.getBySel('select-query-options').within(() => {
    cy.getBySel('add-query-option-btn').click();
    cy.getBySel('query-option-row').should('exist');
    cy.getBySel('query-option-row').within(() => {
      cy.selectFromDropdown({
        testId: 'query-option-select-name',
        value: name,
      });

      cy.fillTextField({
        testId: 'query-option-value-input',
        content: value,
      });
    });
  });
});

Cypress.Commands.add(
  'pinotQlBuilder_AddQueryFilter',
  (params: { columnName: string; operator: string; values: string[] }) => {
    cy.getBySel('select-filters').should('exist');
    cy.getBySel('select-filters').within(() => {
      cy.checkFormLabel({ wantLabel: 'Filters' });

      const addFilterBtn = cy.getBySel('add-filter-btn').should('exist');
      addFilterBtn.click();

      // -- Check filter row --
      cy.getBySel('filter-row').should('exist');
      cy.getBySel('filter-row').within(() => {
        cy.selectFromDropdown({ testId: 'query-filter-column-select', value: params.columnName });
        cy.selectFromDropdown({ testId: 'query-filter-operator-select', value: params.operator });
        params.values.forEach((value) => {
          cy.selectFromDropdown({ testId: 'query-filter-value-select', value });
        });
      });
    });
  }
);

Cypress.Commands.add(
  'setupExplore',
  (params: { dsName: string; queryType: 'PinotQL' | 'PromQL'; editorMode: 'Builder' | 'Code' }) => {
    const { dsName, queryType, editorMode } = params;

    // Visit the Explore page.
    cy.visit('/explore');
    cy.location('pathname').should('eq', '/explore');

    // Set up the query editor.
    cy.selectDatasource(dsName);
    cy.setDashboardTimeRange({ from: '2024-04-01 00:00:00', to: '2024-09-30 23:59:59' });

    // At this point, there should be no data.
    cy.getBySel('explore-no-data').should('exist').and('have.text', 'No data');

    cy.checkRadio({
      testId: 'select-query-type',
      wantLabel: 'Query Type',
      wantOptions: ['PinotQL', 'PromQL'],
    });
    cy.selectFromRadio({ testId: 'select-query-type', option: queryType });

    cy.checkRadio({
      testId: 'select-editor-mode',
      wantOptions: ['Builder', 'Code'],
    });
    cy.selectFromRadio({ testId: 'select-editor-mode', option: editorMode });
  }
);

Cypress.Commands.add('checkRadio', (params: { testId: string; wantLabel?: string; wantOptions?: string[] }) => {
  const { testId, wantLabel, wantOptions } = params;
  cy.get(`[data-testid=${testId}]`).should('exist');
  cy.get(`[data-testid=${testId}]`).within(() => {
    if (wantLabel) {
      cy.checkFormLabel({ wantLabel: wantLabel });
    }
    cy.get('input[type="radio"]').should('exist');
    cy.get('input[type="radio"]')
      .parent()
      .within(() => {
        wantOptions.forEach((option, i) => {
          cy.get('label').eq(i).should('exist').and('contain.text', option);
        });
      });
  });
});

Cypress.Commands.add('selectFromRadio', (params: { testId: string; option: string }) => {
  const { testId, option } = params;
  cy.get(`[data-testid=${testId}]`).should('exist');
  cy.get(`[data-testid=${testId}]`).within(() => {
    cy.get('input[type="radio"]').should('exist');
    cy.get('label').contains(option).should('exist');
    cy.get('label').contains(option).click();
  });
});

Cypress.Commands.add('checkSqlEditor', (params: { wantContent?: string }) => {
  const { wantContent } = params;

  cy.getBySel('sql-editor-container').should('exist');
  cy.getBySel('sql-editor-container').scrollIntoView();
  cy.getBySel('sql-editor-container').within(() => {
    cy.checkFormLabel({ wantLabel: 'Pinot Query' });
    cy.get('[aria-label="Code editor container"]').should('exist');
    cy.get('[aria-label="Code editor container"]').within(() => {
      cy.get('.monaco-editor', { timeout: 5000 }).should('exist');
      cy.window().then((win) => {
        const editor = (win as any).monaco.editor.getModels()[0].getValue();
        if (wantContent !== undefined) {
          cy.wrap(wantContent).should('equal', editor.trim().replace(/ {2}/g, ' '));
        }
      });
    });
  });
});

Cypress.Commands.add('fillSqlEditor', (params: { content: string }) => {
  const { content } = params;

  cy.getBySel('sql-editor-container').should('exist');
  cy.getBySel('sql-editor-container').scrollIntoView();
  cy.getBySel('sql-editor-container').within(() => {
    cy.get('[aria-label="Code editor container"]').should('exist');
    cy.get('[aria-label="Code editor container"]').within(() => {
      cy.get('.monaco-editor', { timeout: 5000 }).should('exist');
      cy.window().then((win) => {
        const editor = (win as any).monaco.editor.getModels()[0];
        editor.setValue(content);
        cy.wrap(content).should('equal', editor.getValue());
      });
    });
  });
});

Cypress.Commands.add('clickRunQuery', () => {
  cy.getBySel('query-editor-header').should('exist');
  cy.getBySel('query-editor-header').within(() => {
    cy.getBySel('run-query-btn').should('exist').and('have.text', 'Run Query');
    cy.getBySel('run-query-btn').click();
  });
});
