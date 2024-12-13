Cypress.Commands.add(
  'createPinotDatasource',
  (params: { controllerUrl: string; brokerUrl: string; databaseName: string; authType: string; authToken: string }) => {
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
  }
);

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

Cypress.Commands.add('fillTextField', (params: { testId: string; content: string }) => {
  const { testId, content } = params;
  cy.wrap(testId).should('not.be.empty');

  cy.get(`[data-testid=${testId}]`).should('exist');
  cy.get(`[data-testid=${testId}]`).within(() => {
    cy.get('input').should('exist');
    cy.get('input').type(content);
    cy.get('input').should('exist').and('have.value', content);
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
      if (wantLabel) {
        cy.checkFormLabel({ wantLabel });
      }
      cy.get('input').should('exist').and('have.value', wantContent);
      if (wantPlaceholder !== undefined) {
        cy.get('input').should('have.attr', 'placeholder', wantPlaceholder);
      }
    });
  }
);

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
