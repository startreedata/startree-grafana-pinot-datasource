import { EnvVariables } from 'support/constants/env-variables';
import { getUniqueString } from 'support/utils/get-unique-string';

interface TestCtx {
  newlyCreatedDatasourceUid: null | string;
  apiResponse: {};
}

describe('Create a Pinot Data Source', () => {
  const ctx: TestCtx = {
    newlyCreatedDatasourceUid: null,
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

  afterEach(() => {
    // Delete newly created data source after tests
    if (ctx.newlyCreatedDatasourceUid) {
      cy.deletePinotDatasource(ctx.newlyCreatedDatasourceUid);
    }
  });

  it('Should create a pinot data source with valid credentials and connection details', () => {
    /**
     * All Intercepts
     */
    cy.intercept('GET', '/api/datasources').as('getDatasources');
    cy.intercept('GET', '/api/plugins?embedded=0').as('pluginsEmbedded');
    cy.intercept('GET', '/api/plugins/errors').as('pluginsErrors');
    cy.intercept('GET', '/api/gnet/plugins').as('gnetPlugins');
    cy.intercept('GET', '/api/plugins?enabled=1&type=datasource').as('pluginsTypeDatasource');
    cy.intercept('POST', '/api/datasources', (req) => {
      req.continue((res) => (ctx.newlyCreatedDatasourceUid = res.body.datasource.uid));
    }).as('postDatasources');
    cy.intercept('GET', '/api/frontend/settings').as('frontendSettings');
    cy.intercept('GET', '/api/plugins/startree-pinot-datasource/settings').as('pluginsSettings');
    cy.intercept('GET', '/api/access-control/user/permissions?reloadcache=true').as('userPermissions');
    cy.intercept('PUT', '/api/datasources/uid/*').as('datasourcesUid');
    cy.intercept('GET', '/api/datasources/uid/*?accesscontrol=true').as('datasourcesUidAccessControl');
    cy.intercept('GET', '/api/datasources/*/health').as('datasourcesHealth');
    cy.intercept('DELETE', '/api/datasources/uid/*').as('deleteDatasource');

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
    cy.wait(['@getDatasources', '@postDatasources', '@frontendSettings']);

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
    cy.get('#basic-settings-name').should('exist').clear().type(formData.name);

    /**
     * Check the Connection heading
     */
    cy.getBySel('connection-heading').should('exist').and('have.text', 'Connection');

    /**
     * Check the initial form errors by clicking save & test button
     */
    cy.get('button').contains('Save & test').should('exist').as('saveAndTestBtn').click();

    // Check for the Page Alert
    cy.get('[data-testid="data-testid Alert error"]').contains(formErrors.brokerUrl).should('be.visible');

    /**
     * Check and fill Controller URL field
     */
    cy.getBySel('controller-url-inline-field')
      .should('exist')
      .within(() => {
        // Check label
        cy.get('label').should('exist').and('have.text', 'Controller URL');

        // Check Input
        cy.get('input').should('exist').and('have.attr', 'placeholder', 'Controller URL').type(formData.controllerUrl);
      });

    /**
     * Check and fill Broker URL field
     */
    cy.getBySel('broker-url-inline-field')
      .should('exist')
      .within(() => {
        // Check label
        cy.get('label').should('exist').and('have.text', 'Broker URL');

        // Check Input
        cy.get('input')
          .should('exist')
          .and('have.attr', 'placeholder', 'Broker URL')
          .and('have.value', formData.brokerUrl);
      });

    /**
     * Check and fill Database field
     */
    cy.getBySel('database-inline-field')
      .should('exist')
      .within(() => {
        // Check label
        cy.get('label').should('exist').and('have.text', 'Database');

        // Check Input
        cy.get('input').should('exist').and('have.attr', 'placeholder', 'default').type(formData.database);
      });

    /**
     * Check Auth details
     */
    cy.getBySel('auth-heading').should('exist').and('have.text', 'Authentication');
    cy.getBySel('auth-description')
      .should('exist')
      .and(
        'contain.text',
        'This plugin requires a Pinot authentication token. For detailed instructions on generating a token, view the documentation.'
      )
      .within(() => {
        cy.getBySel('view-doc-link')
          .should('exist')
          .and('have.text', 'view the documentation')
          .and(
            'have.attr',
            'href',
            'https://dev.startree.ai/docs/query-data/use-apis-and-build-apps/generate-an-api-token'
          )
          .and('have.attr', 'target', '_blank')
          .and('have.attr', 'rel', 'noreferrer');
      });

    /**
     * Check and select Auth Type
     */
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
            ['Basic', 'Bearer', 'None'].forEach((option, i) => {
              cy.get(`#react-select-2-option-${i}`).should('exist').and('have.text', option);
            });

            // Select Auth Type
            cy.contains(formData.authType).parent().click();
          });
      });

    /**
     * Check and fill Auth Token
     */
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
    cy.get('@saveAndTestBtn').click();

    cy.wait(['@datasourcesUid', '@datasourcesUidAccessControl']);

    // Check for the Page Alert
    cy.get('[data-testid="data-testid Alert success"]').contains('Datasource updated').should('be.visible');

    /**
     * Check for data source health
     */
    cy.wait('@datasourcesHealth');

    // Check for page alert
    cy.get('[data-testid="data-testid Alert success"]').contains('Pinot data source is working').should('be.visible');

    /**
     * Finally delete the newly created data source
     */
    cy.get('button').contains('Delete').should('exist').click();
    cy.get('[data-overlay-container="true"]')
      .should('exist')
      .within(() => {
        cy.get('button').contains('Delete').should('exist').click();
      });
    cy.wait('@deleteDatasource').then(({ response }) => {
      if (response.statusCode === 200) {
        ctx.newlyCreatedDatasourceUid = null;
      }
    });

    // Check for success alert and pathname
    cy.get('[data-testid="data-testid Alert success"]').contains('Data source deleted').should('be.visible');
    cy.wait(['@frontendSettings']);
    cy.location('pathname').should('eq', '/datasources');
  });

  it('Saved pinot connection should usable in explore and panel editor', () => {
    /**
     * All Intercepts
     */
    cy.intercept('GET', '/api/datasources').as('getDatasources');
    cy.intercept('GET', '/api/plugins?embedded=0').as('pluginsEmbedded');
    cy.intercept('GET', '/api/plugins/errors').as('pluginsErrors');
    cy.intercept('GET', '/api/gnet/plugins').as('gnetPlugins');
    cy.intercept('GET', '/api/plugins?enabled=1&type=datasource').as('pluginsTypeDatasource');
    cy.intercept('POST', '/api/datasources', (req) => {
      req.continue((res) => (ctx.newlyCreatedDatasourceUid = res.body.datasource.uid));
    }).as('postDatasources');
    cy.intercept('GET', '/api/frontend/settings').as('frontendSettings');
    cy.intercept('GET', '/api/plugins/startree-pinot-datasource/settings').as('pluginsSettings');
    cy.intercept('GET', '/api/access-control/user/permissions?reloadcache=true').as('userPermissions');
    cy.intercept('PUT', '/api/datasources/uid/*').as('datasourcesUid');
    cy.intercept('GET', '/api/datasources/uid/*?accesscontrol=true').as('datasourcesUidAccessControl');
    cy.intercept('GET', '/api/datasources/*/health').as('datasourcesHealth');
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');
    cy.intercept('DELETE', '/api/datasources/uid/*').as('deleteDatasource');
    cy.intercept('GET', '/api/dashboards/home').as('dashboardsHome');
    cy.intercept('GET', '/api/prometheus/grafana/api/v1/rules').as('apiV1Rules');
    cy.intercept('GET', '/api/ruler/grafana/api/v1/rules?subtype=cortex').as('apiV1RulesSubtypeCortex');
    cy.intercept('POST', '/api/ds/query').as('dsQuery');

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

    /**
     * Check for newly created data source in explore page
     */
    cy.get('a').contains('Explore').click();
    cy.location('pathname').should('eq', '/explore');
    cy.wait('@resourcesTables');

    // Check if explore dropdown has newly created data source value
    cy.get('#data-source-picker').should('be.visible').parent().parent().should('contain.text', formData.name).click();
    cy.get('#react-select-3-listbox')
      .should('be.visible')
      .within(() => {
        cy.contains(formData.name).click();
      });

    /**
     * Check for newly created data source in panel editor
     */
    cy.visit('/');
    cy.wait('@dashboardsHome');

    // -- Check and Add Panel --
    cy.get('[aria-label="Add panel"]').click();
    cy.get('button').contains('Add a new panel').click();
    cy.location('search').should('contain', 'editPanel');

    cy.wait(['@apiV1Rules', '@apiV1RulesSubtypeCortex', '@dsQuery', '@resourcesTables']);
    cy.contains('Home / Edit Panel').should('be.visible');

    // --  Check and Data source --
    cy.get('#data-source-picker').should('exist').parent().parent().click();
    cy.get('#react-select-6-listbox')
      .should('be.visible')
      .within(() => {
        cy.contains(formData.name).click();
      });

    // -- Discard the Panel and go back --
    cy.get('button[aria-label="Undo all changes"]').should('exist').click();
    cy.location('search').should('not.contain', 'editPanel');

    /**
     * Finally delete the newly created data source
     */
    cy.get('@newlyCreatedDatasourceUid').then((datasourceUid: unknown) => {
      cy.visit(`/datasources/edit/${datasourceUid}`);
      cy.location('pathname').should('eq', `/datasources/edit/${datasourceUid}`);
    });

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
  });
});
