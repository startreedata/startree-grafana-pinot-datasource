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
    cy.getBySel('inline-form-label').should('exist').and('have.text', 'Sql Preview');
    cy.getBySel('sql-preview').should('exist').and('not.be.empty');
    if (want !== undefined) {
      cy.getBySel('sql-preview').should('have.text', want);
    }
  });
});

Cypress.Commands.add('fillLegend', (legend?: string) => {
  cy.getBySel('metric-legend').should('exist');
  cy.getBySel('metric-legend').within(() => {
    cy.getBySel('inline-form-label').should('exist').and('have.text', 'Legend');

    cy.get('input').should('exist').as('metricLegendInput');

    if (legend !== undefined) {
      cy.get('@metricLegendInput').type(legend);
    }
  });
});

Cypress.Commands.add('selectFromDropdown', (params: { testId: string; value: string }) => {
  const { testId, value } = params;
  cy.get(`[data-testid=${testId}]`).should('exist');
  cy.get(`[data-testid=${testId}]`).within(() => {
    cy.getBySel('inline-form-label').should('exist');

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
  (params: { testId: string; wantLabel: string; wantSelected: string; wantOptions: string[] }) => {
    const { testId, wantLabel, wantSelected, wantOptions } = params;
    cy.get(`[data-testid=${testId}]`).should('exist');
    cy.get(`[data-testid=${testId}]`).within(() => {
      cy.getBySel('inline-form-label').should('exist').and('have.text', wantLabel);

      cy.get('input')
        .parent()
        .parent()
        .as('dropdownSelect')
        .within(() => {
          if (wantSelected) {
            cy.contains(wantSelected);
          }
        });

      cy.get('@dropdownSelect').click();
      cy.wrap(cy.$$('body')).as('body');
      cy.get('@body').find('[aria-label="Select options menu"]').should('be.visible');
      cy.get('@body')
        .find('[aria-label="Select options menu"]')
        .within(() => {
          wantOptions.forEach((option) => cy.contains(option));
        });
    });
  }
);
