type AddVariableTestCtx = {
  newlyCreatedDatasourceUid: null | string;
  apiResponse: {
    resourcesTables?: Record<string, any>;
    resourcesTablesSchema?: Record<string, any>;
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

  it('Columns query should show the list of columns', () => {
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

    const formData = {
      table: 'complex_website',
      columnType: 'ALL',
    };

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

    cy.wait(['@resourcesTables', '@dsQuery']);
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

        // Check and select Columns radio button
        cy.get('input[type="radio"]')
          .should('exist')
          .parent()
          .within(() => {
            cy.get('label').should('exist').contains('Columns').click();
            cy.wait('@dsQuery');
          });
      });

    /**
     * Check initial Preview Values
     */
    cy.contains('Preview of values')
      .parent()
      .parent()
      .as('previewOfValues')
      .within(() => {
        cy.get('label[aria-label="Variable editor Preview of Values option"]')
          .should('exist')
          .and('have.length', 1)
          .contains('None');
      });

    /**
     * Check and select Table
     */
    cy.getBySel('select-table')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Table');

        // Check select list options
        cy.get('input')
          .parent()
          .parent()
          .as('tableSelect')
          .within(() => {
            cy.contains('Choose');
          })
          .click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            const selectOptions = ctx.apiResponse.resourcesTables.tables as string[];
            selectOptions.forEach((option) => cy.contains(option));

            // Select the option
            cy.contains(formData.table).click();
            cy.wait(['@resourcesTablesSchema', '@dsQuery']);
          });

        // Check if correct option is selected
        cy.get('@tableSelect').within(() => {
          cy.contains(formData.table);
        });
      });

    /**
     * Check and select Column Type
     */
    cy.getBySel('select-column-type')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Column Type');

        // Check select list options
        cy.get('input')
          .parent()
          .parent()
          .as('columnTypeSelect')
          .within(() => {
            cy.contains('ALL');
          })
          .click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            const selectOptions = ['DIMENSION', 'METRIC', 'DATETIME', 'ALL'];
            selectOptions.forEach((option) => cy.contains(option));

            // Select the option
            cy.contains(formData.columnType).click();
            cy.wait('@dsQuery').its('response.body').as('dsQueryResp');
          });

        // Check if correct option is selected
        cy.get('@columnTypeSelect').within(() => {
          cy.contains(formData.columnType);
        });
      });

    /**
     * Check Preview of values
     */
    cy.get('@dsQueryResp').then((resp: unknown) => {
      const data = resp as Record<string, any>;
      const previewValues: string[] = data.results.A.frames[0].data.values[0];

      cy.get('@previewOfValues').within(() => {
        cy.get('label[aria-label="Variable editor Preview of Values option"]').should('exist').as('previewValue');

        // Check Preview values
        previewValues.forEach((value) => {
          cy.get('@previewValue').contains(value);
        });
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

  it('Distinct values query should show the list of distinct values', () => {
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
    cy.intercept('GET', '/api/datasources/*/resources/tables/*/schema', (req) => {
      req.continue((res) => (ctx.apiResponse.resourcesTablesSchema = res.body));
    }).as('resourcesTablesSchema');
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/distinctValues').as('sqlDistinctValues');

    const formData = {
      table: 'complex_website',
      column: 'browser',
    };

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

    cy.wait(['@resourcesTables', '@dsQuery']);
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

        // Check and select Distinct Values radio button
        cy.get('input[type="radio"]')
          .should('exist')
          .parent()
          .within(() => {
            cy.get('label').should('exist').contains('Distinct Values').click();
            cy.wait('@dsQuery');
          });
      });

    /**
     * Check initial Preview Values
     */
    cy.contains('Preview of values')
      .parent()
      .parent()
      .as('previewOfValues')
      .within(() => {
        cy.get('label[aria-label="Variable editor Preview of Values option"]')
          .should('exist')
          .and('have.length', 1)
          .contains('None');
      });

    /**
     * Check and select Table
     */
    cy.getBySel('select-table')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Table');

        // Check select list options
        cy.get('input')
          .parent()
          .parent()
          .as('tableSelect')
          .within(() => {
            cy.contains('Choose');
          })
          .click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            const selectOptions = ctx.apiResponse.resourcesTables.tables as string[];
            selectOptions.forEach((option) => cy.contains(option));

            // Select the option
            cy.contains(formData.table).click();
            cy.wait(['@resourcesTablesSchema', '@dsQuery']);
          });

        // Check if correct option is selected
        cy.get('@tableSelect').within(() => {
          cy.contains(formData.table);
        });
      });

    /**
     * Check initial Sql Preview value
     */
    cy.getBySel('sql-preview')
      .should('exist')
      .as('sqlPreview')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Sql Preview');

        cy.getBySel('sql-preview-value').should('exist').and('be.empty');
      });

    /**
     * Check and select Column
     */
    cy.getBySel('select-column')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Column');

        // Check select list options
        cy.get('input')
          .parent()
          .parent()
          .as('columnSelect')
          .within(() => {
            cy.contains('Choose');
          })
          .click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            const tableSchema = ctx.apiResponse.resourcesTablesSchema.schema;

            const selectOptions = [
              ...(tableSchema?.dateTimeFieldSpecs || []),
              ...(tableSchema?.metricFieldSpecs || []),
              ...(tableSchema?.dimensionFieldSpecs || []),
            ].map(({ name }) => name);

            // Check the options length
            cy.wrap(selectOptions).should('have.length.greaterThan', 0);

            // Check the options
            selectOptions.forEach((option) => cy.contains(option));

            // Select the option
            cy.contains(formData.column).click();

            cy.wait('@sqlDistinctValues').its('response.body').as('sqlDistinctValuesResp');
            cy.wait('@dsQuery').its('response.body').as('dsQueryResp');
          });

        // Check if correct option is selected
        cy.get('@columnSelect').within(() => {
          cy.contains(formData.column);
        });
      });

    /**
     * Check Sql Preview value
     */
    cy.get('@sqlPreview').within(() => {
      cy.get('@sqlDistinctValuesResp').then((resp: unknown) => {
        const sqlPreviewValue = (resp as Record<string, any>).sql;

        cy.getBySel('sql-preview-value').should('have.text', sqlPreviewValue.replace(/\n/g, ' '));
      });
    });

    /**
     * Check Preview of values
     */
    cy.get('@dsQueryResp').then((resp: unknown) => {
      const data = resp as Record<string, any>;
      const previewValues: string[] = data.results.A.frames[0].data.values[0];

      cy.get('@previewOfValues').within(() => {
        cy.get('label[aria-label="Variable editor Preview of Values option"]').should('exist').as('previewValue');

        // Check Preview values
        previewValues.forEach((value) => {
          cy.get('@previewValue').contains(value);
        });
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

  it.only('Sql query should show the return data', () => {
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
    cy.intercept('GET', '/api/datasources/*/resources/tables/*/schema', (req) => {
      req.continue((res) => (ctx.apiResponse.resourcesTablesSchema = res.body));
    }).as('resourcesTablesSchema');

    const formData = {
      table: 'complex_website',
      pinotQuery: `SELECT DISTINCT "browser" FROM "complex_website" ORDER BY "browser" ASC LIMIT 100;`,
    };

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

    cy.wait(['@resourcesTables', '@dsQuery']);
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

        // Check and select Sql Query radio button
        cy.get('input[type="radio"]')
          .should('exist')
          .parent()
          .within(() => {
            cy.get('label').should('exist').contains('Sql Query').click();
            cy.wait('@dsQuery');
          });
      });

    /**
     * Check initial Preview Values
     */
    cy.contains('Preview of values')
      .parent()
      .parent()
      .as('previewOfValues')
      .within(() => {
        cy.get('label[aria-label="Variable editor Preview of Values option"]')
          .should('exist')
          .and('have.length', 1)
          .contains('None');
      });

    /**
     * Check and select Table
     */
    cy.getBySel('select-table')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Table');

        // Check select list options
        cy.get('input')
          .parent()
          .parent()
          .as('tableSelect')
          .within(() => {
            cy.contains('Choose');
          })
          .click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            const selectOptions = ctx.apiResponse.resourcesTables.tables as string[];
            selectOptions.forEach((option) => cy.contains(option));

            // Select the option
            cy.contains(formData.table).click();
            cy.wait(['@resourcesTablesSchema', '@dsQuery']);
          });

        // Check if correct option is selected
        cy.get('@tableSelect').within(() => {
          cy.contains(formData.table);
        });
      });

    /**
     * Check and fill Pinot Query
     */
    cy.getBySel('sql-editor')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Pinot Query');

        // Check the sql editor
        cy.get('[aria-label="Code editor container"]')
          .should('exist')
          .within(() => {
            cy.get('.monaco-editor', { timeout: 5000 }).should('exist');

            cy.window()
              .then((win) => {
                // Access the Monaco Editor instance via the window object
                const editor = (win as any).monaco.editor.getModels()[0]; // Get the first model instance
                const editorValue = editor.getValue(); // Retrieve the editor's content

                // Check the initial query value
                cy.wrap(editorValue).should('be.empty');

                // Set the new pinot query value
                editor.setValue(formData.pinotQuery.trim());

                // Check if query editor has the new value
                const editorNewValue = editor.getValue();
                cy.wrap(formData.pinotQuery.trim().replace(/ /g, ''))
                  .should('equal', editorNewValue.trim().replace(/ /g, ''))
                  .then(() => {
                    cy.log('Then after wrap');
                  });
              })
              .then(() => {
                cy.log('Then after window');
              });
          })
          .then(() => {
            /**
             * Check Preview of values
             */
            cy.wait('@dsQuery', { timeout: 5000 }).then(({ response }) => {
              const data = response.body as Record<string, any>;
              cy.log('Check: ', JSON.stringify(response.body));
              const previewValues: string[] = data.results.A.frames[0].data.values[0];

              cy.get('@previewOfValues').within(() => {
                cy.get('label[aria-label="Variable editor Preview of Values option"]')
                  .should('exist')
                  .as('previewValue');

                // Check Preview values
                previewValues.forEach((value) => {
                  cy.get('@previewValue').contains(value);
                });
              });
            });
          });
      })
      .then(() => {
        cy.log('Then after sql-editor');
      });

    cy.log('Then after all');

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
