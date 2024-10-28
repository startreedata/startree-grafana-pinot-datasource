import { getUniqueString } from 'support/utils/get-unique-string';
import { createPinotDatasource, deletePinotDatasource } from './create-pinot-datasource';

export interface TestCtx {
  panelTitle: string;
  newlyCreatedDatasourceUid: null | string;
  apiResponse: {
    resourcesTables?: Record<string, any>;
  };
}

describe('Create a Panel with Pinot Code Editor', () => {
  const ctx: TestCtx = {
    panelTitle: `test_e2e_panel_${getUniqueString(5)}`,
    newlyCreatedDatasourceUid: null,
    apiResponse: {},
  };

  afterEach(() => {
    // Delete newly created data source after tests
    if (ctx.newlyCreatedDatasourceUid) {
      cy.deletePinotDatasource(ctx.newlyCreatedDatasourceUid);
    }
  });

  it('User should be able to create a Panel using Pinot Code Editor', () => {
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
    cy.intercept('GET', '/api/datasources/*/resources/tables', (req) => {
      req.continue((res) => (ctx.apiResponse.resourcesTables = res.body));
    }).as('resourcesTables');
    cy.intercept('DELETE', '/api/datasources/uid/*').as('deleteDatasource');
    cy.intercept('GET', '/api/dashboards/home').as('dashboardsHome');
    cy.intercept('GET', '/api/prometheus/grafana/api/v1/rules').as('apiV1Rules');
    cy.intercept('GET', '/api/ruler/grafana/api/v1/rules?subtype=cortex').as('apiV1RulesSubtypeCortex');
    cy.intercept('POST', '/api/ds/query').as('dsQuery');

    const formData = {
      displayType: 'Time Series',
      table: 'complex_website',
      timeAlias: 'time',
      metricAlias: 'metric',
      pinotQuery: `
        SELECT
            $__timeGroup("hoursSinceEpoch") AS $__timeAlias(),
            SUM("views") AS $__metricAlias()
        FROM $__table()
        WHERE $__timeFilter("hoursSinceEpoch")
        GROUP BY $__timeGroup("hoursSinceEpoch")
        ORDER BY $__timeAlias() DESC
        LIMIT 1000
      `,
      legend: null,
    };

    /**
     * Granting the clipboard permissions to browser
     */
    cy.wrap(
      Cypress.automation('remote:debugger:protocol', {
        command: 'Browser.grantPermissions',
        params: {
          permissions: ['clipboardReadWrite', 'clipboardSanitizedWrite'],
          origin: window.location.origin,
        },
      })
    );

    /**
     * Create new Pinot Datasource for testing create panel flow
     */
    createPinotDatasource(ctx).then((data) => {
      cy.wrap({
        name: data.name,
        uid: ctx.newlyCreatedDatasourceUid,
      }).as('newlyCreatedDatasource');
    });

    /**
     * Visit Dashboard page and initialize
     */
    cy.visit('/');
    cy.wait('@dashboardsHome');

    /**
     * Check and Add Panel
     */
    cy.get('[aria-label="Add panel"]').should('be.visible').click();
    cy.get('button').contains('Add a new panel').should('be.visible').click();
    cy.location('search').should('contain', 'editPanel');

    cy.wait(['@apiV1Rules', '@apiV1RulesSubtypeCortex', '@dsQuery', '@resourcesTables']);
    cy.contains('Home / Edit Panel').should('be.visible');

    /**
     * Change Panel Title
     */
    cy.get('input#PanelFrameTitle').should('exist').clear().type(ctx.panelTitle);

    /**
     * Change the Time Range
     */
    cy.get('[data-testid="data-testid TimePicker Open Button"]').should('exist').click();
    cy.get('#TimePickerContent')
      .should('be.visible')
      .within(() => {
        // Fill from time field
        cy.get('input[aria-label="Time Range from field"]').should('exist').clear().type('2024-04-01 00:00:00');

        // Fill to time field
        cy.get('input[aria-label="Time Range to field"]').should('exist').clear().type('2024-09-30 23:59:59');

        // Apply time range
        cy.get('button').contains('Apply time range').click();
      });

    /**
     * Check and select Data source
     */
    cy.get('@newlyCreatedDatasource').then((data: unknown) => {
      const pinotDatasourceName: string = (data as any).name;

      cy.get('#data-source-picker').should('exist').parent().parent().as('dataSourcePicker').click();
      cy.get('[aria-label="Select options menu"]')
        .should('be.visible')
        .within(() => {
          cy.contains(pinotDatasourceName).click();
        });

      // Check the selected data source
      cy.get('@dataSourcePicker').should('contain.text', pinotDatasourceName);
    });

    /**
     * Check Select Query Type
     */
    cy.getBySel('select-query-type')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Query Type');

        // Check Radio group
        cy.get('input[type="radio"]')
          .should('exist')
          .parent()
          .within(() => {
            cy.get('label').should('exist').and('contain.text', 'PinotQL');
          });
      });

    /**
     * Check and Select Editor Mode
     */
    cy.getBySel('select-editor-mode')
      .should('exist')
      .within(() => {
        // Check Radio group
        cy.get('input[type="radio"]')
          .eq(1)
          .should('exist')
          .invoke('attr', 'id')
          .then((id) => {
            cy.get(`label[for="${id}"]`).should('exist').and('contain.text', 'Code').click();
          });
      });

    cy.wait('@dsQuery');

    /**
     * Check Run query button
     */
    cy.getBySel('query-editor-header')
      .should('exist')
      .within(() => {
        cy.getBySel('run-query-btn').should('exist').and('have.text', 'Run Query').as('runQueryBtn');
      });

    /**
     * Check and fill Display Type field
     */
    cy.getBySel('select-display-type')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Display');

        // Check Radio group buttons
        ['Table', 'Time Series'].forEach((option, i) => {
          cy.get('input[type="radio"]')
            .eq(i)
            .should('exist')
            .invoke('attr', 'id')
            .then((id) => {
              cy.get(`label[for="${id}"]`).should('exist').and('contain.text', option);
            });
        });

        if (formData.displayType) {
          cy.get('label').contains(formData.displayType).click();
        }
      });

    /**
     * Check and fill Time Alias field
     */
    cy.getBySel('time-column-alias')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Time Alias');

        cy.get('input').should('exist').as('timeAliasInput');

        if (formData.timeAlias) {
          cy.get('@timeAliasInput').type(formData.timeAlias);
        }
      });

    /**
     * Check and fill Metric Alias field
     */
    cy.getBySel('metric-column-alias')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Metric Alias');

        cy.get('input').should('exist').as('metricAliasInput');

        if (formData.metricAlias) {
          cy.get('@metricAliasInput').type(formData.metricAlias);
        }
      });

    /**
     * Check and fill Pinot Query field
     */
    cy.getBySel('sql-editor-container')
      .should('exist')
      .scrollIntoView()
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Pinot Query');

        cy.get('[aria-label="Code editor container"]')
          .should('exist')
          .within(() => {
            cy.get('.monaco-editor', { timeout: 5000 }).should('exist');

            cy.window().then((win) => {
              // Access the Monaco Editor instance via the window object
              const editor = (win as any).monaco.editor.getModels()[0]; // Get the first model instance
              const editorValue = editor.getValue(); // Retrieve the editor's content

              const defaultValue = `
                SELECT $__timeGroup("timestamp") AS $__timeAlias(), SUM("metric") AS $__metricAlias()
                FROM $__table()
                WHERE $__timeFilter("timestamp")
                GROUP BY $__timeGroup("timestamp")
                ORDER BY $__timeAlias() DESC
                LIMIT 100000
              `;

              // Check the default query value
              cy.wrap(defaultValue.trim().replace(/ /g, '')).should('equal', editorValue.trim().replace(/ /g, ''));

              // Set the new pinot query value
              editor.setValue(formData.pinotQuery.trim());

              // Check if query editor has the new value
              const editorNewValue = editor.getValue();
              cy.wrap(formData.pinotQuery.trim().replace(/ /g, '')).should(
                'equal',
                editorNewValue.trim().replace(/ /g, '')
              );
            });
          });
      });

    /**
     * Check and select Table field
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
            const tables = ctx.apiResponse.resourcesTables.tables as string[];
            tables.forEach((option) => cy.contains(option));

            // Select table option
            cy.contains(formData.table).click();
          });

        // Check if correct option is selected
        cy.get('@tableSelect').within(() => {
          cy.contains(formData.table);
        });
      });

    /**
     * Check SQL preview
     */
    cy.getBySel('sql-preview-container')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Sql Preview');

        cy.getBySel('sql-preview').as('sqlPreview').should('exist').and('not.be.empty');

        // Check the limit should be equal to changed limit from form data
        cy.get('@sqlPreview').should('contain.text', 'LIMIT 1000');

        // Check the copy button in SQL Preview
        cy.get('@sqlPreview').within(() => {
          cy.getBySel('copy-query-btn').should('exist').click();
        });

        // Check if the clipboard has the query copied
        cy.get('@sqlPreview')
          .invoke('text')
          .then((sqlPreviewValue) => {
            cy.window().then(async (win) => {
              const text = await win.navigator.clipboard.readText();
              expect(text).to.eq(sqlPreviewValue);
            });
          });
      });

    /**
     * Check and fill Metric Legend field
     */
    cy.getBySel('metric-legend')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Legend');

        cy.get('input').should('exist').as('metricLegendInput');

        if (formData.legend) {
          cy.get('@metricLegendInput').type(formData.legend);
        }
      });

    /**
     * Finally Run Query and check results
     */
    cy.get('@runQueryBtn').click();
    cy.wait('@dsQuery', { timeout: 5000 });

    // Check the UPlot chart
    cy.get('.panel-content').should('not.contain', 'No data');
    cy.getBySel('uplot-main-div').should('exist');

    /**
     * Discard the Panel and go back
     */
    cy.get('button[aria-label="Undo all changes"]').should('exist').click();
    cy.location('search').should('not.contain', 'editPanel');

    /**
     * Delete the newly created data source for the panel
     */
    cy.get('@newlyCreatedDatasource').then((data: unknown) => {
      const datasourceUid = (data as any).uid;
      deletePinotDatasource(ctx, datasourceUid);
    });
  });

  it('Click table in Pinot Code Editor should render a normal table', () => {
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
    cy.intercept('GET', '/api/datasources/*/resources/tables', (req) => {
      req.continue((res) => (ctx.apiResponse.resourcesTables = res.body));
    }).as('resourcesTables');
    cy.intercept('DELETE', '/api/datasources/uid/*').as('deleteDatasource');
    cy.intercept('GET', '/api/dashboards/home').as('dashboardsHome');
    cy.intercept('GET', '/api/prometheus/grafana/api/v1/rules').as('apiV1Rules');
    cy.intercept('GET', '/api/ruler/grafana/api/v1/rules?subtype=cortex').as('apiV1RulesSubtypeCortex');
    cy.intercept('POST', '/api/ds/query').as('dsQuery');

    const formData = {
      displayType: 'Table',
      table: 'complex_website',
      timeAlias: 'time',
      metricAlias: 'metric',
      pinotQuery: `
        SELECT
            $__timeGroup("hoursSinceEpoch") AS $__timeAlias(),
            SUM("views") AS $__metricAlias()
        FROM $__table()
        WHERE $__timeFilter("hoursSinceEpoch")
        GROUP BY $__timeGroup("hoursSinceEpoch")
        ORDER BY $__timeAlias() DESC
        LIMIT 1000
      `,
      legend: null,
    };

    /**
     * Create new Pinot Datasource for testing create panel flow
     */
    createPinotDatasource(ctx).then((data) => {
      cy.wrap({
        name: data.name,
        uid: ctx.newlyCreatedDatasourceUid,
      }).as('newlyCreatedDatasource');
    });

    /**
     * Visit Dashboard page and initialize
     */
    cy.visit('/');
    cy.wait('@dashboardsHome');

    /**
     * Check and Add Panel
     */
    cy.get('[aria-label="Add panel"]').should('be.visible').click();
    cy.get('button').contains('Add a new panel').should('be.visible').click();
    cy.location('search').should('contain', 'editPanel');

    cy.wait(['@apiV1Rules', '@apiV1RulesSubtypeCortex', '@dsQuery', '@resourcesTables']);
    cy.contains('Home / Edit Panel').should('be.visible');

    /**
     * Change Panel Title
     */
    cy.get('input#PanelFrameTitle').should('exist').clear().type(ctx.panelTitle);

    /**
     * Change the Time Range
     */
    cy.get('[data-testid="data-testid TimePicker Open Button"]').should('exist').click();
    cy.get('#TimePickerContent')
      .should('be.visible')
      .within(() => {
        // Fill from time field
        cy.get('input[aria-label="Time Range from field"]').should('exist').clear().type('2024-04-01 00:00:00');

        // Fill to time field
        cy.get('input[aria-label="Time Range to field"]').should('exist').clear().type('2024-09-30 23:59:59');

        // Apply time range
        cy.get('button').contains('Apply time range').click();
      });

    /**
     * Change Visualization
     */
    cy.get('button[aria-label="toggle-viz-picker"]').should('exist').click();

    cy.get('[aria-label="Panel editor option pane content"]')
      .should('be.visible')
      .within(() => {
        cy.get('div[aria-label="Plugin visualization item Table"]').should('exist').click();
      });

    cy.get('button[aria-label="toggle-viz-picker"]').should('contain.text', 'Table');

    /**
     * Check and select Data source
     */
    cy.get('@newlyCreatedDatasource').then((data: unknown) => {
      const pinotDatasourceName: string = (data as any).name;

      cy.get('#data-source-picker').should('exist').parent().parent().as('dataSourcePicker').click();
      cy.get('[aria-label="Select options menu"]')
        .should('be.visible')
        .within(() => {
          cy.contains(pinotDatasourceName).click();
        });

      // Check the selected data source
      cy.get('@dataSourcePicker').should('contain.text', pinotDatasourceName);
    });

    /**
     * Check and Select Editor Mode
     */
    cy.getBySel('select-editor-mode')
      .should('exist')
      .within(() => {
        // Check Radio group
        cy.get('input[type="radio"]')
          .eq(1)
          .invoke('attr', 'id')
          .then((id) => {
            cy.get(`label[for="${id}"]`).should('exist').and('contain.text', 'Code').click();
          });
      });

    cy.wait('@dsQuery');

    /**
     * Check Run query button
     */
    cy.getBySel('query-editor-header')
      .should('exist')
      .within(() => {
        cy.getBySel('run-query-btn').as('runQueryBtn');
      });

    /**
     * Check and fill Display Type field
     */
    cy.getBySel('select-display-type')
      .should('exist')
      .within(() => {
        // Select Display Type Table
        cy.get('label').contains(formData.displayType).click();
      });

    /**
     * Check and fill Time Alias field
     */
    cy.getBySel('time-column-alias')
      .should('exist')
      .within(() => {
        if (formData.timeAlias) {
          cy.get('input').type(formData.timeAlias);
        }
      });

    /**
     * Check and fill Metric Alias field
     */
    cy.getBySel('metric-column-alias')
      .should('exist')
      .within(() => {
        if (formData.metricAlias) {
          cy.get('input').type(formData.metricAlias);
        }
      });

    /**
     * Check and fill Pinot Query field
     */
    cy.getBySel('sql-editor-container')
      .should('exist')
      .within(() => {
        cy.get('[aria-label="Code editor container"]').within(() => {
          cy.get('.monaco-editor', { timeout: 5000 }).should('exist');

          cy.window().then((win) => {
            // Access the Monaco Editor instance via the window object
            const editor = (win as any).monaco.editor.getModels()[0]; // Get the first model instance

            // Set the new pinot query value
            editor.setValue(formData.pinotQuery.trim());

            // Check if query editor has the new value
            const editorNewValue = editor.getValue();
            cy.wrap(formData.pinotQuery.trim().replace(/ /g, '')).should(
              'equal',
              editorNewValue.trim().replace(/ /g, '')
            );
          });
        });
      });

    /**
     * Check and select Table field
     */
    cy.getBySel('select-table')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input').parent().parent().as('tableSelect').click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            // Select table option
            cy.contains(formData.table).click();
            cy.wait('@dsQuery');
          });

        // Check if correct option is selected
        cy.get('@tableSelect').within(() => {
          cy.contains(formData.table);
        });
      });

    /**
     * Check SQL preview
     */
    cy.getBySel('sql-preview-container')
      .should('exist')
      .within(() => {
        cy.getBySel('sql-preview').should('not.be.empty');
      });

    /**
     * Check and fill Metric Legend field
     */
    cy.getBySel('metric-legend')
      .should('exist')
      .within(() => {
        if (formData.legend) {
          cy.get('input').type(formData.legend);
        }
      });

    /**
     * Finally Run Query and check results
     */
    cy.get('@runQueryBtn').click();
    cy.wait('@dsQuery', { timeout: 5000 }).then(({ response }) => {
      const fields = response.body.results.A.frames[0].schema.fields;

      // Check the result data
      cy.wrap(fields[0]).should('have.property', 'name', 'timetime');
      cy.wrap(fields[1]).should('have.property', 'name', 'metric');
    });

    // Check the rendered Table
    cy.get('section.panel-container')
      .should('exist')
      .last()
      .and('not.contain', 'No data')
      .within(() => {
        cy.get('div[role="table"]').should('exist');
      });

    /**
     * Discard the Panel and go back
     */
    cy.get('button[aria-label="Undo all changes"]').should('exist').click();
    cy.location('search').should('not.contain', 'editPanel');

    /**
     * Delete the newly created data source for the panel
     */
    cy.get('@newlyCreatedDatasource').then((data: unknown) => {
      const datasourceUid = (data as any).uid;
      deletePinotDatasource(ctx, datasourceUid);
    });
  });

  it('Table view should render when selecting different types of tables', () => {
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
    cy.intercept('GET', '/api/datasources/*/resources/tables', (req) => {
      req.continue((res) => (ctx.apiResponse.resourcesTables = res.body));
    }).as('resourcesTables');
    cy.intercept('DELETE', '/api/datasources/uid/*').as('deleteDatasource');
    cy.intercept('GET', '/api/dashboards/home').as('dashboardsHome');
    cy.intercept('GET', '/api/prometheus/grafana/api/v1/rules').as('apiV1Rules');
    cy.intercept('GET', '/api/ruler/grafana/api/v1/rules?subtype=cortex').as('apiV1RulesSubtypeCortex');
    cy.intercept('POST', '/api/ds/query').as('dsQuery');

    const formData = {
      displayType: 'Table',
      timeAlias: 'time',
      metricAlias: 'metric',
      legend: null,
    };

    /**
     * Create new Pinot Datasource for testing create panel flow
     */
    createPinotDatasource(ctx).then((data) => {
      cy.wrap({
        name: data.name,
        uid: ctx.newlyCreatedDatasourceUid,
      }).as('newlyCreatedDatasource');
    });

    /**
     * Visit Dashboard page and initialize
     */
    cy.visit('/');
    cy.wait('@dashboardsHome');

    /**
     * Check and Add Panel
     */
    cy.get('[aria-label="Add panel"]').should('be.visible').click();
    cy.get('button').contains('Add a new panel').should('be.visible').click();
    cy.location('search').should('contain', 'editPanel');

    cy.wait(['@apiV1Rules', '@apiV1RulesSubtypeCortex', '@dsQuery']);
    cy.wait('@resourcesTables').its('response.body').as('resourcesTablesResp');
    cy.contains('Home / Edit Panel').should('be.visible');

    /**
     * Change Panel Title
     */
    cy.get('input#PanelFrameTitle').should('exist').clear().type(ctx.panelTitle);

    /**
     * Change the Time Range
     */
    cy.get('[data-testid="data-testid TimePicker Open Button"]').should('exist').click();
    cy.get('#TimePickerContent')
      .should('be.visible')
      .within(() => {
        // Fill from time field
        cy.get('input[aria-label="Time Range from field"]').should('exist').clear().type('2024-04-01 00:00:00');

        // Fill to time field
        cy.get('input[aria-label="Time Range to field"]').should('exist').clear().type('2024-09-30 23:59:59');

        // Apply time range
        cy.get('button').contains('Apply time range').click();
      });

    /**
     * Check and select Data source
     */
    cy.get('@newlyCreatedDatasource').then((data: unknown) => {
      const pinotDatasourceName: string = (data as any).name;

      cy.get('#data-source-picker').should('exist').parent().parent().as('dataSourcePicker').click();
      cy.get('[aria-label="Select options menu"]')
        .should('be.visible')
        .within(() => {
          cy.contains(pinotDatasourceName).click();
        });

      // Check the selected data source
      cy.get('@dataSourcePicker').should('contain.text', pinotDatasourceName);
    });

    /**
     * Check and Select Editor Mode
     */
    cy.getBySel('select-editor-mode')
      .should('exist')
      .within(() => {
        // Check Radio group
        cy.get('input[type="radio"]')
          .eq(1)
          .invoke('attr', 'id')
          .then((id) => {
            cy.get(`label[for="${id}"]`).should('exist').and('contain.text', 'Code').click();
          });
      });

    cy.wait('@dsQuery');

    /**
     * Check Run query button
     */
    cy.getBySel('query-editor-header')
      .should('exist')
      .within(() => {
        cy.getBySel('run-query-btn').as('runQueryBtn');
      });

    /**
     * Check and fill Display Type field
     */
    cy.getBySel('select-display-type')
      .should('exist')
      .within(() => {
        if (formData.displayType) {
          cy.get('label').contains(formData.displayType).click();
        }
      });

    /**
     * Check and fill Time Alias field
     */
    cy.getBySel('time-column-alias')
      .should('exist')
      .within(() => {
        if (formData.timeAlias) {
          cy.get('input').type(formData.timeAlias);
        }
      });

    /**
     * Check and fill Metric Alias field
     */
    cy.getBySel('metric-column-alias')
      .should('exist')
      .within(() => {
        if (formData.metricAlias) {
          cy.get('input').type(formData.metricAlias);
        }
      });

    /**
     * Check Pinot Query field
     */
    cy.getBySel('sql-editor-container')
      .should('exist')
      .as('sqlEditorContainer')
      .within(() => {
        cy.get('[aria-label="Code editor container"]')
          .should('exist')
          .as('codeEditorContainer')
          .within(() => {
            cy.get('.monaco-editor', { timeout: 5000 }).should('exist');
          });
      });

    /**
     * Check and select Table field
     */
    cy.getBySel('select-table')
      .should('exist')
      .as('selectTableRow')
      .within(() => {
        cy.get('input').parent().parent().as('tableSelect');
      });

    /**
     * Check and fill Metric Legend field
     */
    cy.getBySel('metric-legend')
      .should('exist')
      .within(() => {
        if (formData.legend) {
          cy.get('input').type(formData.legend);
        }
      });

    /**
     * Run query for different types of table and check the results
     */
    cy.get('@resourcesTablesResp').then((resourcesTables: unknown) => {
      const tables: string[] = (resourcesTables as any).tables;

      tables.forEach((table) => {
        /**
         * Select table
         */
        cy.get('@selectTableRow').within(() => {
          cy.get('@tableSelect').click();

          cy.wrap(cy.$$('body'))
            .find('[aria-label="Select options menu"]')
            .should('be.visible')
            .within(() => {
              // Select table option
              cy.contains(table).click();
            });

          // Check if correct option is selected
          cy.get('@tableSelect').within(() => {
            cy.contains(table);
          });
        });

        /**
         * Check and fill Pinot Query field
         */
        cy.get('@sqlEditorContainer').within(() => {
          cy.get('@codeEditorContainer').within(() => {
            cy.get('.monaco-editor', { timeout: 5000 }).should('exist');

            cy.window().then((win) => {
              const pinotQuery = `
                SELECT * FROM ${table};
              `;

              // Access the Monaco Editor instance via the window object
              const editor = (win as any).monaco.editor.getModels()[0];

              // Set the new pinot query value
              editor.setValue(pinotQuery.trim());

              // Check if query editor has the new value
              const editorNewValue = editor.getValue();
              cy.wrap(pinotQuery.trim().replace(/ /g, '')).should('equal', editorNewValue.trim().replace(/ /g, ''));
            });
          });
        });

        /**
         * Check SQL Preview
         */
        cy.getBySel('sql-preview-container')
          .should('exist')
          .within(() => {
            cy.getBySel('sql-preview').should('not.be.empty');
          });

        /**
         * Run Query and check results
         */
        cy.get('@runQueryBtn').click();
        cy.wait('@dsQuery', { timeout: 5000 });

        // Check the UPlot chart
        cy.get('.panel-content').should('not.contain', 'No data');
      });
    });

    /**
     * Discard the Panel and go back
     */
    cy.get('button[aria-label="Undo all changes"]').should('exist').click();
    cy.location('search').should('not.contain', 'editPanel');

    /**
     * Delete the newly created data source for the panel
     */
    cy.get('@newlyCreatedDatasource').then((data: unknown) => {
      const datasourceUid = (data as any).uid;
      deletePinotDatasource(ctx, datasourceUid);
    });
  });
});
