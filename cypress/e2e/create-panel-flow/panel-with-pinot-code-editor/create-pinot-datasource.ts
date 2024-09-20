import { EnvVariables } from 'support/constants/env-variables';
import type { TestCtx } from './panel-with-pinot-code-editor.cy';
import { getUniqueString } from 'support/utils/get-unique-string';

interface FormData {
  name: string;
  controllerUrl: string;
  brokerUrl: string;
  database: string;
  authType: 'Bearer' | 'Basic' | 'None';
  authToken: string;
}

export const createPinotDatasource = (
  ctx: TestCtx,
  initialFormData: Partial<FormData> = {}
): Cypress.Chainable<FormData> => {
  const formData: FormData = {
    name: `Pinot_e2e_${getUniqueString(5)}`,
    controllerUrl: Cypress.env(EnvVariables.pinotConnectionControllerUrl),
    brokerUrl: Cypress.env(EnvVariables.pinotConnectionBrokerUrl),
    database: Cypress.env(EnvVariables.pinotConnectionDatabase),
    authType: 'Bearer',
    authToken: Cypress.env(EnvVariables.pinotConnectionAuthToken),
    ...initialFormData,
  };

  /**
   * Visit page and initialize
   */
  cy.visit('/datasources');
  cy.wait('@getDatasources');

  /**
   * Check Add Data Source
   */
  cy.get('a').contains('Add data source').click();
  cy.location('pathname').should('eq', '/datasources/new');

  cy.wait(['@pluginsEmbedded', '@pluginsErrors', '@gnetPlugins', '@pluginsTypeDatasource']);

  /**
   * Check Filter data source and select pinot data source
   */
  cy.getBySel('input-wrapper').within(() => {
    cy.get('input').type('Pinot');
  });

  cy.get('button').contains('Pinot').click();
  cy.wait('@getDatasources');
  cy.wait('@postDatasources').its('response.body.datasource.uid').as('newlyCreatedDatasourceUid');
  cy.wait('@frontendSettings');

  /**
   * Check Added data source
   */
  cy.location('pathname').should('match', /\/datasources\/edit\/.*/);
  cy.get('[data-testid="data-testid Alert success"]').contains('Datasource added').should('be.visible');

  cy.wait(['@userPermissions', '@datasourcesUidAccessControl', '@pluginsSettings']);

  /**
   * Check and fill Name field
   * Note: this is not part of the plugin but needs to change the data source name
   */
  cy.get('#basic-settings-name').clear().type(formData.name);

  /**
   * Check and fill Controller URL field
   */
  cy.getBySel('controller-url-inline-field').within(() => {
    cy.get('input').type(formData.controllerUrl);
  });

  /**
   * Check and fill Broker URL field
   */
  cy.getBySel('broker-url-inline-field').within(() => {
    cy.get('input').and('have.value', formData.brokerUrl);
  });

  /**
   * Check and fill Database field
   */
  cy.getBySel('database-inline-field').within(() => {
    cy.get('input').type(formData.database);
  });

  /**
   * Check and select Auth Type
   */
  cy.getBySel('auth-type-container').within(() => {
    // Check Select
    cy.get('#react-select-2-input').parent().parent().click();

    cy.wrap(cy.$$('body'))
      .find('#react-select-2-listbox')
      .within(() => {
        // Select Auth Type
        cy.contains(formData.authType).parent().click();
      });
  });

  /**
   * Check and fill Auth Token
   */
  cy.getBySel('auth-token-container').within(() => {
    cy.get('input').type(formData.authToken);
  });

  /**
   * Check Save and test data source
   */
  cy.get('button').contains('Save & test').click();

  cy.wait(['@datasourcesUid', '@datasourcesUidAccessControl']);

  // Check for the Page Alert
  cy.get('[data-testid="data-testid Alert success"]').contains('Datasource updated').should('be.visible');

  /**
   * Check for data source health
   */
  cy.wait('@datasourcesHealth');

  // Check for page alert
  cy.get('[data-testid="data-testid Alert success"]').contains('Pinot data source is working').should('be.visible');

  return cy.wrap({ ...formData }).then((data: unknown) => {
    return data as FormData;
  });
};

export const deletePinotDatasource = (ctx: TestCtx, datasourceUid: string) => {
  cy.visit(`/datasources/edit/${datasourceUid}`);
  cy.location('pathname').should('eq', `/datasources/edit/${datasourceUid}`);

  cy.wait(['@datasourcesUidAccessControl', '@pluginsSettings']);

  // Check for Delete button and click
  cy.get('button').contains('Delete').click();
  cy.get('[data-overlay-container="true"]').within(() => {
    cy.get('button').contains('Delete').click();
  });

  // Wait for delete request to complete
  cy.wait('@deleteDatasource').then(({ response }) => {
    if (response.statusCode === 200) {
      ctx.newlyCreatedDatasourceUid = null;
    }
  });

  // Check for success alert and pathname
  cy.get('[data-testid="data-testid Alert success"]').contains('Data source deleted').should('be.visible');
  cy.wait(['@frontendSettings']);
  cy.location('pathname').should('eq', '/datasources');
};
