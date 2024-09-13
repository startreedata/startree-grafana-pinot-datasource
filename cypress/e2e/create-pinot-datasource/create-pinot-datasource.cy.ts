import { EnvVariables } from 'support/constants/env-variables';
import { getUniqueString } from 'support/utils/get-unique-string';

interface TestCtx {
  apiResponse: {
    getDatasources?: Array<Record<string, unknown>>;
  };
}

describe('Create a Pinot Data Source', () => {
  const ctx: TestCtx = {
    apiResponse: {},
  };

  const formData = {
    name: `Pinot_e2e_${getUniqueString(5)}`,
    controllerUrl: Cypress.env(EnvVariables.pinotConnectionControllerUrl),
    brokerUrl: Cypress.env(EnvVariables.pinotConnectionBrokerUrl),
    database: Cypress.env(EnvVariables.pinotConnectionDatabase),
    authType: 'Bearer',
    authToken: Cypress.env(EnvVariables.pinotConnectionAuthToken),
  };

  const formErrors = {
    controllerUrl: 'controller url cannot be empty',
    brokerUrl: 'broker url cannot be empty',
  };

  after(() => {
    //
  });

  it('Should create a pinot data source with valid credentials and connection details', () => {
    /**
     * All Intercepts
     */
    cy.intercept('GET', '/api/datasources', (req) => {
      req.continue((res) => (ctx.apiResponse.getDatasources = res.body));
    }).as('getDatasources');
    cy.intercept('GET', '/api/plugins?embedded=0').as('pluginsEmbedded');
    cy.intercept('GET', '/api/plugins/errors').as('pluginsErrors');
    cy.intercept('GET', '/api/gnet/plugins').as('gnetPlugins');
    cy.intercept('GET', '/api/plugins?enabled=1&type=datasource').as('pluginsTypeDatasource');
    cy.intercept('POST', '/api/datasources').as('postDatasources');
    cy.intercept('PUT', '/api/datasources/uid/*').as('datasourcesUid');
    cy.intercept('GET', '/api/datasources/uid/*?accesscontrol=true').as('datasourcesUidAccessControl');
    cy.intercept('GET', '/api/datasources/*/health').as('datasourcesHealth');

    /**
     * Visit page and initialize
     */
    cy.visit('/datasources');
    cy.wait('@getDatasources');

    /**
     * Check Add Data Source
     */
    cy.get('a').contains('Add data source').should('exist').click();
    cy.location('pathname').should('eq', '/datasources/new');

    cy.wait(['@pluginsEmbedded', '@pluginsErrors', '@gnetPlugins', '@pluginsTypeDatasource']);

    /**
     * Check Filter data source and select pinot data source
     */
    cy.getBySel('input-wrapper')
      .should('be.visible')
      .within(() => {
        cy.get('input').type('Pinot');
      });

    cy.get('button').contains('Pinot').should('be.visible').click();

    /**
     * Check Added data source and save & test
     */
    cy.location('pathname').should('match', /\/datasources\/edit\/.*/);
    cy.get('[data-testid="data-testid Alert success"]').contains('Datasource added').should('be.visible');

    // -- Check Controller Url Input and label --
    cy.getBySel('controller_url')
      .should('exist')
      .and('have.attr', 'placeholder', 'Controller URL')
      .type(formData.controllerUrl);

    // -- Check Broker Url Input and label --
    cy.getBySel('broker_url')
      .should('exist')
      .and('have.attr', 'placeholder', 'Broker URL')
      .and('have.value', formData.brokerUrl);

    // -- Check Database Input and label --
    cy.getBySel('database').should('exist').and('have.attr', 'placeholder', 'default').type(formData.database);

    // -- Check and select Auth Type --
    cy.getBySel('auth-type-container')
      .should('exist')
      .within(() => {
        // Check label
        cy.get('label').should('exist').and('have.text', 'Type');

        // Check Select
        cy.get('#react-select-2-input').should('exist').parent().parent().should('contain', 'Basic').click();
        cy.wrap(cy.$$('body'))
          .find('#react-select-2-listbox')
          .should('be.visible')
          .within(() => {
            // Check Auth Type options

            // Select Auth Type
            cy.contains(formData.authType).parent().click();
          });
      });

    // -- Check and fill Auth Token --
    cy.getBySel('auth-token-container')
      .should('exist')
      .within(() => {
        // Check label
        cy.get('label').should('exist').and('have.text', 'Token');

        // Check input
        cy.get('input')
          .should('exist')
          .and('have.attr', 'type', 'password')
          .and('have.attr', 'placeholder', 'Token')
          .type(formData.authToken);
      });

    /**
     * Check Save and test data source
     */
    cy.get('button').contains('Save & test').should('exist').click();

    cy.wait(['@datasourcesUid', '@datasourcesUidAccessControl']);

    // Check for the Page Alert
    cy.get('[data-testid="data-testid Alert success"]').contains('Datasource updated').should('be.visible');

    /**
     * Check for data source health
     */
    cy.wait('@datasourcesHealth');

    // Check for page alert
    cy.get('[data-testid="data-testid Alert success"]').contains('Pinot data source is working').should('be.visible');
  });
});
