type AddVariableTestCtx = {
  newlyCreatedDatasourceUid: null | string;
  apiResponse: {
    resourcesTables?: Record<string, any>;
  };
};

describe('Add variable with Variable Query Editor', () => {
  const ctx: AddVariableTestCtx = {
    newlyCreatedDatasourceUid: null,
    apiResponse: {},
  };

  afterEach(() => {
    // Delete newly created data source after tests
    if (ctx.newlyCreatedDatasourceUid) {
      cy.deletePinotDatasource(ctx.newlyCreatedDatasourceUid);
    }
  });

  it('Tables query should show the list of tables', () => {
    /**
     * All Intercepts
     */
    // Required for create and delete pinot datasource
    cy.intercept('GET', '/api/datasources').as('getDatasources');
    cy.intercept('GET', '/api/plugins?embedded=0').as('pluginsEmbedded');
    cy.intercept('GET', '/api/plugins/errors').as('pluginsErrors');
    cy.intercept('GET', '/api/gnet/plugins').as('gnetPlugins');
    cy.intercept('GET', '/api/plugins?enabled=1&type=datasource').as('pluginsTypeDatasource');
    cy.intercept('POST', '/api/datasources', (req) => {
      req.continue((res) => (ctx.newlyCreatedDatasourceUid = res.body.datasource.uid));
    }).as('postDatasources');
    cy.intercept('GET', '/api/frontend/settings').as('frontendSettings');
    cy.intercept('GET', '/api/access-control/user/permissions?reloadcache=true').as('userPermissions');
    cy.intercept('GET', '/api/datasources/uid/*?accesscontrol=true').as('datasourcesUidAccessControl');
    cy.intercept('GET', '/api/plugins/startree-pinot-datasource/settings').as('pluginsSettings');
    cy.intercept('PUT', '/api/datasources/uid/*').as('datasourcesUid');
    cy.intercept('GET', '/api/datasources/*/health').as('datasourcesHealth');
    cy.intercept('DELETE', '/api/datasources/uid/*').as('deleteDatasource');
    // Required for flow
    cy.intercept('GET', '/api/datasources/*/resources/tables', (req) => {
      req.continue((res) => (ctx.apiResponse.resourcesTables = res.body));
    }).as('resourcesTables');
    cy.intercept('POST', '/api/ds/query').as('dsQuery');
    cy.intercept('GET', '/api/datasources/*/resources/tables/*/schema').as('resourcesTablesSchema');
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/distinctValues').as('sqlDistinctValues');

    /**
     * Create new Pinot Datasource for testing create panel flow
     */
    cy.createPinotDatasource().then((data) => {
      cy.wrap({
        name: data.name,
        uid: ctx.newlyCreatedDatasourceUid,
      }).as('newlyCreatedDatasource');
    });

    /**
     * Visit New Dashboard page
     */
    cy.visit('/dashboard/new');
    cy.location('pathname').should('eq', '/dashboard/new');

    /**
     * Check and click Dashboard settings
     */
    cy.get('[aria-label="Dashboard settings"]').should('exist').click();
    cy.location('search').should('contain', 'editview=settings');
    cy.get('[aria-label="Search links"]').should('exist');

    /**
     * Check Variables link and click
     */
    cy.get('a').contains('Variables').click();
    cy.location('search').should('contain', 'editview=templating');

    cy.get('button[data-testid="data-testid Call to action button Add variable"]')
      .should('exist')
      .and('contain.text', 'Add variable')
      .click();

    cy.wait('@resourcesTables');
    cy.wait('@dsQuery').its('response.body').as('dsQueryResp');
    cy.get('form[aria-label="Variable editor Form"]').should('exist');

    /**
     * Check and select Data source
     */
    cy.get('@newlyCreatedDatasource').then((data: unknown) => {
      const pinotDatasourceName: string = (data as any).name;

      cy.get('[aria-label="Data source picker select container"]').should('exist').as('dataSourcePicker').click();

      cy.get('[aria-label="Select options menu"]')
        .should('be.visible')
        .within(() => {
          cy.contains(pinotDatasourceName).click();
        });

      // Check the selected data source
      cy.get('@dataSourcePicker').should('contain.text', pinotDatasourceName);
    });

    /**
     * Check Select Variable Type
     */
    cy.getBySel('select-variable-type')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Type');

        // Check and select Tables radio button
        cy.get('input[type="radio"]')
          .should('exist')
          .parent()
          .within(() => {
            cy.get('label').should('exist').contains('Tables').click();
          });
      });

    /**
     * Check Preview of values
     */
    cy.contains('Preview of values').parent().parent().as('previewOfValues');

    cy.get('@dsQueryResp').then((resp: unknown) => {
      const data = resp as Record<string, any>;
      const previewValues: string[] = data.results.A.frames[0].data.values[0];

      cy.get('@previewOfValues').within(() => {
        cy.get('label[aria-label="Variable editor Preview of Values option"]').should('exist').as('previewValue');

        // Check Preview values
        previewValues.forEach((value) => {
          cy.get('@previewValue').contains(value);
        });

        // Check specifically for complex_website value
        cy.get('@previewValue').contains('complex_website');
      });
    });

    /**
     * Delete the newly created data source for the panel
     */
    cy.get('@newlyCreatedDatasource').then((data: unknown) => {
      const datasourceUid = (data as any).uid;
      cy.deletePinotDatasourceWithUi(datasourceUid).then((result) => {
        if (result.success) {
          ctx.newlyCreatedDatasourceUid = null;
        }
      });
    });
  });
});
