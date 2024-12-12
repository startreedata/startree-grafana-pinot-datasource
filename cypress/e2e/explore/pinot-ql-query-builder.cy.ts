import { EnvVariables } from '../../support/constants/env-variables';

describe('Visualize With Query Builder in Explore', () => {
  const ctx: ExplorePinotTestCtx = {
    newlyCreatedDatasourceUid: null,
    apiResponse: {},
  };

  beforeEach(() => {
    cy.createPinotDatasource({
      controllerUrl: Cypress.env(EnvVariables.pinotConnectionControllerUrl),
      brokerUrl: Cypress.env(EnvVariables.pinotConnectionBrokerUrl),
      databaseName: Cypress.env(EnvVariables.pinotConnectionDatabase),
      authType: 'Bearer',
      authToken: Cypress.env(EnvVariables.pinotConnectionAuthToken),
    }).as('newlyCreatedDatasource');
  });

  afterEach(() => {
    cy.get<{ uid: string }>('@newlyCreatedDatasource').then(({ uid }) => {
      cy.deletePinotDatasource(uid);
    });
  });

  const setupExplore = (dsName: string) => {
    // Visit the Explore page.
    cy.visit('/explore');
    cy.location('pathname').should('eq', '/explore');

    // Set up the query editor.
    cy.selectDatasource(dsName);
    cy.setDashboardTimeRange({ from: '2024-04-01 00:00:00', to: '2024-09-30 23:59:59' });

    // At this point, there should be no data.
    cy.getBySel('explore-no-data').should('exist').and('have.text', 'No data');

    cy.getBySel('select-query-type').should('exist');
    cy.getBySel('select-query-type').within(() => {
      cy.getBySel('inline-form-label').should('exist').and('have.text', 'Query Type');
      cy.get('input[type="radio"]').should('exist');
      cy.get('input[type="radio"]')
        .parent()
        .within(() => {
          cy.get('label').eq(0).should('exist').and('contain.text', 'PinotQL');
          cy.get('label').eq(1).should('exist').and('contain.text', 'PromQL');
        });
    });

    cy.get('[data-testid=select-editor-mode]').should('exist');
    cy.getBySel('select-editor-mode').within(() => {
      cy.get('input[type="radio"]').should('exist');
      cy.get('input[type="radio"]')
        .parent()
        .within(() => {
          cy.get('label').eq(0).should('exist').and('contain.text', 'Builder');
          cy.get('label').eq(0).click({ force: true });
        });
    });
  };

  const formData = {
    table: 'complex_website',
    timeColumn: 'hoursSinceEpoch',
    granularity: null,
    metricColumn: 'views',
    aggregation: 'SUM',
    groupBy: ['country'],
    orderBy: [],
    filters: [{ column: 'country', operator: '=', value: 'CN' }],
    queryOptions: [],
    limit: 1000,
    legend: null,
  };

  it('Fully populates the table dropdown', () => {
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) => {
      setupExplore(ds.name);
    });

    cy.wait('@resourcesTables');
    cy.checkDropdown({
      testId: 'select-table',
      wantLabel: 'Table',
      wantSelected: '',
      wantOptions: ['complex_website', 'simple_website'],
    });
  });

  it('Fully populates the time column dropdown', () => {
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');
    cy.intercept('POST', '/api/datasources/*/resources/columns').as('resourcesColumns');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) => {
      setupExplore(ds.name);
    });

    cy.wait('@resourcesTables');
    cy.selectFromDropdown({ testId: 'select-table', value: 'complex_website' });

    cy.wait('@resourcesColumns');
    cy.checkDropdown({
      testId: 'select-time-column',
      wantLabel: 'Time Column',
      wantSelected: 'hoursSinceEpoch',
      wantOptions: ['hoursSinceEpoch'],
    });
  });

  it('Fully populates the granularity dropdown', () => {
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');
    cy.intercept('POST', '/api/datasources/*/resources/columns').as('resourcesColumns');
    cy.intercept('POST', '/api/datasources/*/resources/granularities').as('resourcesGranularities');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) => {
      setupExplore(ds.name);
    });

    cy.wait('@resourcesTables');
    cy.selectFromDropdown({ testId: 'select-table', value: 'complex_website' });

    cy.wait('@resourcesColumns');
    cy.selectFromDropdown({ testId: 'select-time-column', value: 'hoursSinceEpoch' });

    cy.wait('@resourcesGranularities');
    cy.checkDropdown({
      testId: 'select-granularity',
      wantLabel: 'Granularity',
      wantSelected: 'auto',
      wantOptions: ['auto', 'DAYS', 'HOURS'],
    });
  });

  it('Fully populates the metric column dropdown', () => {
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');
    cy.intercept('POST', '/api/datasources/*/resources/columns').as('resourcesColumns');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) => {
      setupExplore(ds.name);
    });

    cy.wait('@resourcesTables');
    cy.selectFromDropdown({ testId: 'select-table', value: 'complex_website' });

    cy.wait('@resourcesColumns');
    cy.checkDropdown({
      testId: 'select-metric-column',
      wantLabel: 'Metric Column',
      wantSelected: 'views',
      wantOptions: ['views', 'clicks', 'errors'],
    });
  });

  it('Fully populates the aggregation dropdown', () => {
    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) => {
      setupExplore(ds.name);
    });
    cy.checkDropdown({
      testId: 'select-aggregation',
      wantLabel: 'Aggregation',
      wantSelected: 'SUM',
      wantOptions: ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'NONE'],
    });
  });

  it('Fully populates the group by dropdown', () => {
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');
    cy.intercept('POST', '/api/datasources/*/resources/columns').as('resourcesColumns');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) => {
      setupExplore(ds.name);
    });

    cy.wait('@resourcesTables');
    cy.selectFromDropdown({ testId: 'select-table', value: 'complex_website' });

    cy.wait('@resourcesColumns');
    cy.selectFromDropdown({ testId: 'select-metric-column', value: 'clicks' });

    cy.checkDropdown({
      testId: 'select-group-by',
      wantLabel: 'Group By',
      wantSelected: '',
      wantOptions: ['country', 'browser', 'platform', 'views', 'errors'],
    });
  });

  it('Fully populates the order by dropdown', () => {
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');
    cy.intercept('POST', '/api/datasources/*/resources/columns').as('resourcesColumns');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) => {
      setupExplore(ds.name);
    });

    cy.wait('@resourcesTables');
    cy.selectFromDropdown({ testId: 'select-table', value: 'complex_website' });

    cy.wait('@resourcesColumns');
    cy.selectFromDropdown({ testId: 'select-metric-column', value: 'clicks' });
    cy.selectFromDropdown({ testId: 'select-group-by', value: 'country' });
    cy.selectFromDropdown({ testId: 'select-group-by', value: 'browser' });

    cy.checkDropdown({
      testId: 'select-order-by',
      wantLabel: 'Order By',
      wantSelected: '',
      wantOptions: ['country asc', 'country desc', 'browser asc', 'browser desc'],
    });
  });

  it('Renders graph with minimum selected fields', () => {
    cy.intercept('POST', '/api/ds/query').as('dsQuery');
    cy.intercept('POST', '/api/datasources/*/resources/columns').as('columns');
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/builder').as('previewSqlBuilder');
    cy.intercept('POST', '/api/datasources/*/resources/query/distinctValues').as('queryDistinctValues');
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) => {
      setupExplore(ds.name);
    });

    cy.wait('@resourcesTables');
    cy.selectFromDropdown({ testId: 'select-table', value: 'complex_website' });

    cy.wait(['@columns', '@previewSqlBuilder', '@dsQuery']);
    cy.pinotQlBuilder_CheckSqlPreview();

    cy.getBySel('explore-no-data').should('not.exist');
    cy.get('div').contains('Graph').should('exist');
    cy.get('[aria-label="Explore Table"]').should('exist');
  });

  it('Graph and Table should render using Pinot Query Builder', () => {
    cy.intercept('POST', '/api/ds/query').as('dsQuery');
    cy.intercept('POST', '/api/datasources/*/resources/columns').as('columns');
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/builder').as('previewSqlBuilder');
    cy.intercept('POST', '/api/datasources/*/resources/granularities').as('resourcesGranularities');
    cy.intercept('POST', '/api/datasources/*/resources/query/distinctValues').as('queryDistinctValues');
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) => {
      setupExplore(ds.name);
    });

    cy.wait('@resourcesTables');
    cy.selectFromDropdown({ testId: 'select-table', value: 'complex_website' });

    cy.wait(['@columns', '@previewSqlBuilder', '@dsQuery']);

    cy.selectFromDropdown({ testId: 'select-time-column', value: 'hoursSinceEpoch' });

    cy.wait('@resourcesGranularities');
    cy.selectFromDropdown({ testId: 'select-granularity', value: 'HOURS' });

    cy.selectFromDropdown({ testId: 'select-metric-column', value: 'clicks' });
    cy.selectFromDropdown({ testId: 'select-aggregation', value: 'AVG' });
    cy.selectFromDropdown({ testId: 'select-group-by', value: 'country' });
    cy.selectFromDropdown({ testId: 'select-group-by', value: 'browser' });
    cy.selectFromDropdown({ testId: 'select-order-by', value: 'browser asc' });
    cy.selectFromDropdown({ testId: 'select-order-by', value: 'country asc' });

    /**
     * Check and select Filters field
     */
    cy.getBySel('select-filters').should('exist');
    cy.getBySel('select-filters').within(() => {
      cy.wrap(cy.$$('body')).as('body');

      // Check form label
      cy.getBySel('inline-form-label').should('exist').and('have.text', 'Filters');

      // Check add filter button
      cy.getBySel('add-filter-btn').should('exist').as('addFilterBtn');
      cy.getBySel('add-filter-btn').click();
      cy.wait(['@dsQuery', '@previewSqlBuilder']);

      // -- Check filter row --
      cy.getBySel('filter-row').should('exist');
      cy.getBySel('filter-row').within(() => {
        // -- Check select column --
        cy.get('#column-select').should('exist');
        cy.get('#column-select').within(() => {
          cy.get('input').should('exist');
          cy.get('input')
            .parent()
            .parent()
            .within(() => {
              cy.contains('Select column');
            });
          cy.get('input').parent().parent().click();

          cy.get('@body').find('[aria-label="Select options menu"]').should('be.visible');
          cy.get('@body')
            .find('[aria-label="Select options menu"]')
            .within(() => {
              const selectOptions = ctx.apiResponse.columns.result.filter(
                (col) => col.name !== formData.metricColumn && !col.isTime
              );
              selectOptions.forEach((option) => cy.contains(option.name));

              // Close select menu
              cy.get('@body').click(0, 0);
            });
        });

        // -- Check query segment operator select --
        cy.get('#query-segment-operator-select').should('exist');
        cy.get('#query-segment-operator-select').within(() => {
          cy.get('input').should('exist');
          cy.get('input')
            .parent()
            .parent()
            .within(() => {
              cy.contains('Choose');
            });
          cy.get('input').parent().parent().click();

          cy.get('@body').find('[aria-label="Select options menu"]').should('be.visible');
          cy.get('@body')
            .find('[aria-label="Select options menu"]')
            .within(() => {
              const selectOptions = ['=', '!=', '>', '>=', '<', '<=', 'like', 'not like'];

              selectOptions.forEach((option) => cy.contains(option));

              // Close select menu
              cy.get('@body').click(0, 0);
            });
        });

        // -- Check value select --
        cy.get('#value-select').should('exist');
        cy.get('#value-select').within(() => {
          cy.get('input').should('exist');
          cy.get('input')
            .parent()
            .parent()
            .within(() => {
              cy.contains('Select value');
            });
          cy.get('input').parent().parent().click();

          cy.get('@body').find('[aria-label="Select options menu"]').should('be.visible');
          cy.get('@body')
            .find('[aria-label="Select options menu"]')
            .within(() => {
              const selectOptions = ['No options found'];

              selectOptions.forEach((option) => cy.contains(option));

              // Close select menu
              cy.get('@body').click(0, 0);
            });
        });

        // -- Check filter delete button --
        cy.getBySel('delete-filter-btn').should('exist');
        cy.getBySel('delete-filter-btn').click();
        cy.wait(['@dsQuery', '@previewSqlBuilder']);
      });

      // Check filter row should not exits after deleting the row
      cy.getBySel('filter-row').should('not.exist');

      // -- Add the form data if any --
      if (formData.filters && formData.filters.length > 0) {
        formData.filters.forEach((filterOption) => {
          // Add filter row
          cy.get('@addFilterBtn').click();
          cy.wait(['@dsQuery', '@previewSqlBuilder']);

          // -- Check filter row --
          cy.getBySel('filter-row').should('exist');
          cy.getBySel('filter-row').within(() => {
            // Select column
            cy.get('#column-select').within(() => {
              cy.get('input').parent().parent().as('columnSelect').click();

              cy.get('@body').find('[aria-label="Select options menu"]').should('be.visible');
              cy.get('@body')
                .find('[aria-label="Select options menu"]')
                .within(() => {
                  // Select the option
                  cy.contains(filterOption.column).click();
                  cy.wait('@dsQuery');
                });

              // Check if the option is selected
              cy.get('@columnSelect').within(() => {
                cy.contains(filterOption.column);
              });
            });

            // Select query segment operator
            cy.get('#query-segment-operator-select').within(() => {
              cy.get('input')
                .parent()
                .parent()
                .as('querySegmentOperatorSelect')
                .within(() => {
                  // Check pre selected option
                  cy.contains('=');
                });
              cy.get('@querySegmentOperatorSelect').click();

              cy.get('@body').find('[aria-label="Select options menu"]').should('be.visible');
              cy.get('@body')
                .find('[aria-label="Select options menu"]')
                .within(() => {
                  // Select the option
                  cy.contains(filterOption.operator).click();
                  cy.wait('@dsQuery');
                });

              // Check if the option is selected
              cy.get('@querySegmentOperatorSelect').within(() => {
                cy.contains(filterOption.operator);
              });
            });

            // Select value
            cy.get('#value-select').within(() => {
              cy.get('input').parent().parent().as('valueSelect').click();

              cy.wait('@queryDistinctValues').then(({ response }) => {
                const data = response.body as { code: number; result: string[] };
                cy.log('data: ', data);

                cy.get('@body').find('[aria-label="Select options menu"]').should('be.visible');
                cy.get('@body')
                  .find('[aria-label="Select options menu"]')
                  .within(() => {
                    // Check the available options
                    data.result.forEach((valueExpr) => {
                      cy.contains(valueExpr);
                    });

                    // Select the option
                    cy.contains(filterOption.value).click();
                    cy.wait('@dsQuery');
                  });
              });

              // Check if the option is selected
              cy.get('@valueSelect').within(() => {
                cy.contains(filterOption.value);
              });
            });
          });
        });
      }
    });

    /**
     * Check and select Query Option field
     */
    cy.getBySel('select-query-options').should('exist');
    cy.getBySel('select-query-options').within(() => {
      cy.wrap(cy.$$('body')).as('body');

      // Check form label
      cy.getBySel('inline-form-label').should('exist').and('have.text', 'Query Options');

      // Check add query option button
      cy.getBySel('add-query-option-btn').should('exist').as('addQueryOptionBtn');
      cy.getBySel('add-query-option-btn').click();
      cy.wait(['@dsQuery', '@previewSqlBuilder']);

      // -- Check query option row --
      cy.getBySel('query-option-row').should('exist');
      cy.getBySel('query-option-row').within(() => {
        // Check SET label
        cy.getBySel('set-label').should('exist').and('have.text', 'SET');

        // Check query option select
        cy.get('#query-option-select').should('exist');
        cy.get('#query-option-select').within(() => {
          cy.get('input').should('exist');
          cy.get('input')
            .parent()
            .parent()
            .within(() => {
              cy.contains('Choose');
            });
          cy.get('input').click();

          cy.get('@body').find('[aria-label="Select options menu"]').should('be.visible');
          cy.get('@body')
            .find('[aria-label="Select options menu"]')
            .within(() => {
              const selectOptions = [
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
              ];

              selectOptions.forEach((option) => cy.contains(option));

              // Close select menu
              cy.get('@body').click(0, 0);
            });
        });

        // Check operator label
        cy.getBySel('operator-label').should('exist').and('have.text', '=');

        // Check query option input value
        cy.get('#query-option-value-input').should('exist');

        // Check query option delete button
        cy.getBySel('delete-query-option-btn').should('exist');
        cy.getBySel('delete-query-option-btn').click();
        cy.wait(['@dsQuery', '@previewSqlBuilder']);
      });

      // Check query option row should not exits after deleting the row
      cy.getBySel('query-option-row').should('not.exist');

      // -- Add the form data if any --
      if (formData.queryOptions && formData.queryOptions.length > 0) {
        formData.queryOptions.forEach((queryOption) => {
          // Add query option row
          cy.get('@addQueryOptionBtn').click();
          cy.wait(['@dsQuery', '@previewSqlBuilder']);

          // -- Check query option row --
          cy.getBySel('query-option-row').should('exist');
          cy.getBySel('query-option-row').within(() => {
            // Select the query option
            cy.get('#query-option-select').within(() => {
              cy.get('input').parent().parent().click();

              cy.get('@body').find('[aria-label="Select options menu"]').should('be.visible');
              cy.get('@body')
                .find('[aria-label="Select options menu"]')
                .within(() => {
                  // Select the option
                  cy.contains(queryOption.option).click();
                  cy.wait('@dsQuery');
                });
            });

            // Fill the input value
            cy.get('#query-option-value-input').type(queryOption.value.toString());
            cy.wait(['@dsQuery', '@previewSqlBuilder']);
          });
        });
      }
    });

    /**
     * Check and fill Limit field
     */
    cy.getBySel('input-limit').should('exist');
    cy.getBySel('input-limit').within(() => {
      cy.getBySel('inline-form-label').should('exist').and('have.text', 'Limit');
      cy.get('input').should('exist').and('have.attr', 'placeholder', 'auto');
      if (formData.limit != null) {
        cy.get('input').type(formData.limit.toString());
      }
    });

    /**
     * Check Sql Preview Container
     */
    cy.pinotQlBuilder_CheckSqlPreview();

    /**
     * Check and fill Metric Legend field
     */
    cy.getBySel('metric-legend').should('exist');
    cy.getBySel('metric-legend').within(() => {
      cy.getBySel('inline-form-label').should('exist').and('have.text', 'Legend');

      cy.get('input').should('exist').as('metricLegendInput');

      if (formData.legend) {
        cy.get('@metricLegendInput').type(formData.legend);
      }
    });

    /**
     * Finally Run Query and check results
     */

    cy.getBySel('query-editor-header').should('exist');
    cy.getBySel('query-editor-header').within(() => {
      cy.getBySel('run-query-btn').should('exist').and('have.text', 'Run Query').as('runQueryBtn');

      cy.get('@runQueryBtn').click();
      cy.wait('@dsQuery', { timeout: 5000 });
      cy.wait('@previewSqlBuilder');

      // Check the No data message for graph and table should not exist
      cy.getBySel('explore-no-data').should('not.exist');

      // Check the Graph div
      cy.get('div').contains('Graph').should('exist');

      // Check the Table div
      cy.get('[aria-label="Explore Table"]').should('exist');
    });
  });
});
