import { getUniqueString } from 'support/utils/get-unique-string';
import { createPinotDatasource, deletePinotDatasource } from './create-pinot-datasource';

export interface TestCtx {
  panelTitle: string;
  newlyCreatedDatasourceUid: null | string;
  apiResponse: {
    resourcesTables?: Record<string, any>;
    tablesSchema?: Record<string, any>;
    dsQuery?: Record<string, any>;
  };
}

describe('Create a Panel with Pinot Query Builder', () => {
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

  it('User should be able to create a Panel using Pinot Query Builder', () => {
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
    cy.intercept('GET', '/api/datasources/*/resources/tables/*/schema', (req) => {
      req.continue((res) => (ctx.apiResponse.tablesSchema = res.body));
    }).as('tablesSchema');
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/builder').as('previewSqlBuilder');
    cy.intercept('POST', '/api/datasources/*/resources/query/distinctValues').as('queryDistinctValues');

    const formData = {
      table: 'complex_website',
      timeColumn: 'hoursSinceEpoch',
      granularity: null,
      metricColumn: 'clicks',
      aggregation: 'SUM',
      groupBy: ['country'],
      orderBy: [],
      filters: [{ column: 'country', operator: '=', value: 'CN' }],
      queryOptions: [],
      limit: 1000,
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
        cy.contains('Last 6 months').parent().click();
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
          .eq(0)
          .should('exist')
          .invoke('attr', 'id')
          .then((id) => {
            cy.get(`label[for="${id}"]`).should('exist').and('contain.text', 'Builder').click();
          });
      });

    /**
     * Check Run query button
     */
    cy.getBySel('query-editor-header')
      .should('exist')
      .within(() => {
        cy.getBySel('run-query-btn').should('exist').and('have.text', 'Run Query').as('runQueryBtn');
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
            const selectOptions = ctx.apiResponse.resourcesTables.tables as string[];
            selectOptions.forEach((option) => cy.contains(option));

            // Select the option
            cy.contains(formData.table).click();
          });

        // Check if correct option is selected
        cy.get('@tableSelect').within(() => {
          cy.contains(formData.table);
        });
      });

    cy.wait(['@tablesSchema', '@previewSqlBuilder', '@dsQuery']);

    /**
     * Check and select Time Column field
     */
    cy.getBySel('select-time-column')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Time Column');

        // Check select list options
        cy.get('input')
          .parent()
          .parent()
          .as('timeColumnSelect')
          .within(() => {
            // Check already selected option
            cy.contains('hoursSinceEpoch');
          })
          .click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            const selectOptions = (ctx.apiResponse.tablesSchema.schema as Record<string, unknown>)
              .dateTimeFieldSpecs as Array<Record<string, string>>;

            selectOptions.forEach((option) => cy.contains(option.name));

            // Select the option
            cy.contains(formData.timeColumn).click();
          });

        // Check if correct option is selected
        cy.get('@timeColumnSelect').within(() => {
          cy.contains(formData.timeColumn);
        });
      });

    /**
     * Check and select Granularity field
     */
    cy.getBySel('select-granularity')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Granularity');

        // Check select list options
        cy.get('input')
          .parent()
          .parent()
          .as('granularitySelect')
          .within(() => {
            // Check already selected option
            cy.contains('auto');
          })
          .click();

        cy.wrap(cy.$$('body'))
          .as('body')
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            const selectOptions = [
              'auto',
              'DAYS',
              'HOURS',
              'MINUTES',
              'SECONDS',
              'MILLISECONDS',
              'MICROSECONDS',
              'NANOSECONDS',
            ];

            selectOptions.forEach((option) => cy.contains(option));

            // Select the option
            if (formData.granularity) {
              cy.contains(formData.granularity).click();

              cy.wait(['@dsQuery', '@previewSqlBuilder']);
            } else {
              // Close the select menu
              cy.get('@body').click(0, 0);
            }
          });

        // Check if correct option is selected
        if (formData.granularity) {
          cy.get('@granularitySelect').within(() => {
            cy.contains(formData.granularity);
          });
        }
      });

    /**
     * Check and select Metric Column field
     */
    cy.getBySel('select-metric-column')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Metric Column');

        // Check select list options
        cy.get('input')
          .parent()
          .parent()
          .as('metricColumnSelect')
          .within(() => {
            // Check already selected option
            cy.contains('clicks');
          })
          .click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            const selectOptions = (ctx.apiResponse.tablesSchema.schema as Record<string, unknown>)
              .metricFieldSpecs as Array<Record<string, string>>;

            selectOptions.forEach((option) => cy.contains(option.name));

            // Select the option
            cy.contains(formData.metricColumn).click();
            cy.wait(['@dsQuery', '@previewSqlBuilder']);
          });

        // Check if correct option is selected
        cy.get('@metricColumnSelect').within(() => {
          cy.contains(formData.metricColumn);
        });
      });

    /**
     * Check and select Aggregation field
     */
    cy.getBySel('select-aggregation')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Aggregation');

        // Check select list options
        cy.get('input')
          .parent()
          .parent()
          .as('aggregationSelect')
          .within(() => {
            // Check already selected option
            cy.contains('SUM');
          })
          .click();

        cy.wrap(cy.$$('body'))
          .as('body')
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            const selectOptions = ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'NONE'];

            selectOptions.forEach((option) => cy.contains(option));

            // Select the option
            if (formData.aggregation) {
              cy.contains(formData.aggregation).click();

              cy.wait(['@dsQuery', '@previewSqlBuilder']);
            } else {
              // Close the select menu
              cy.get('@body').click(0, 0);
            }
          });

        // Check if correct option is selected
        if (formData.aggregation) {
          cy.get('@aggregationSelect').within(() => {
            cy.contains(formData.aggregation);
          });
        }
      });

    /**
     * Check and select Group By field
     */
    cy.getBySel('select-group-by')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Group By');

        // Check select list options
        cy.get('input')
          .parent()
          .parent()
          .as('groupBySelect')
          .within(() => {
            cy.contains('Choose');
          })
          .click();

        cy.wrap(cy.$$('body')).as('body');

        cy.get('@body')
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            const schema = ctx.apiResponse.tablesSchema.schema as Record<string, unknown>;
            const dimensionFieldSpecs = schema.dimensionFieldSpecs as Array<Record<string, string>>;
            const metricFieldSpecs = schema.metricFieldSpecs as Array<Record<string, string>>;

            const selectOptions = [...dimensionFieldSpecs, ...metricFieldSpecs].filter(
              (item) => item.name !== formData.metricColumn
            );

            selectOptions.forEach((option) => cy.contains(option.name));

            // Select the first option
            cy.contains(selectOptions[0].name).click();
            cy.wait(['@dsQuery', '@previewSqlBuilder']);

            // Check if the first option is selected
            cy.get('@groupBySelect').within(() => {
              cy.contains(selectOptions[0].name)
                .parent()
                .within(() => {
                  // Check if remove button exist for selected option
                  cy.get(`[aria-label="Remove ${selectOptions[0].name}"]`).should('exist').click();
                });

              cy.wait(['@dsQuery', '@previewSqlBuilder']);

              cy.contains(selectOptions[0].name).should('not.exist');
            });
          });

        // Select the form data options
        if (formData.groupBy && formData.groupBy.length > 0) {
          formData.groupBy.forEach((option) => {
            // Open select menu
            cy.get('@groupBySelect').click();

            cy.get('@body')
              .find('[aria-label="Select options menu"]')
              .should('be.visible')
              .within(() => {
                // Select the option
                cy.contains(option).click();

                cy.wait(['@dsQuery', '@previewSqlBuilder']);
              });

            // Check if option is selected
            cy.get('@groupBySelect').within(() => {
              cy.contains(option);
            });
          });
        }
      });

    /**
     * Check and select Order By field
     */
    cy.getBySel('select-order-by')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Order By');

        // Check select list options
        cy.get('input')
          .parent()
          .parent()
          .as('orderBySelect')
          .within(() => {
            cy.contains('Choose');
          })
          .click();

        cy.wrap(cy.$$('body')).as('body');

        cy.get('@body')
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            const selectOptions = ['time asc', 'time desc', 'metric asc', 'metric desc'];
            formData.groupBy.forEach((groupByOption) => {
              selectOptions.push(`${groupByOption} asc`, `${groupByOption} desc`);
            });

            selectOptions.forEach((option) => cy.contains(option));

            // Select the first option
            cy.contains(selectOptions[0]).click();
            cy.wait(['@dsQuery', '@previewSqlBuilder']);

            // Check if the first option is selected
            cy.get('@orderBySelect').within(() => {
              cy.contains(selectOptions[0])
                .parent()
                .within(() => {
                  // Check if remove button exist for selected option
                  cy.get(`[aria-label="Remove ${selectOptions[0]}"]`).should('exist').click();
                });

              cy.wait(['@dsQuery', '@previewSqlBuilder']);

              cy.contains(selectOptions[0]).should('not.exist');
            });
          });

        // Select the form data options
        if (formData.orderBy && formData.orderBy.length > 0) {
          formData.orderBy.forEach((option) => {
            // Open select menu
            cy.get('@orderBySelect').click();

            cy.get('@body')
              .find('[aria-label="Select options menu"]')
              .should('be.visible')
              .within(() => {
                // Select the option
                cy.contains(option).click();

                cy.wait(['@dsQuery', '@previewSqlBuilder']);
              });

            // Check if option is selected
            cy.get('@orderBySelect').within(() => {
              cy.contains(option);
            });
          });
        }
      });

    /**
     * Check and select Filters field
     */
    cy.getBySel('select-filters')
      .should('exist')
      .within(() => {
        cy.wrap(cy.$$('body')).as('body');

        // Check form label
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Filters');

        // Check add filter button
        cy.getBySel('add-filter-btn').should('exist').as('addFilterBtn').click();
        cy.wait(['@dsQuery', '@previewSqlBuilder']);

        /**
         * Check filter row and default options
         */
        cy.getBySel('filter-row')
          .should('exist')
          .within(() => {
            // -- Check select column --
            cy.get('#column-select')
              .should('exist')
              .within(() => {
                cy.get('input')
                  .should('exist')
                  .parent()
                  .parent()
                  .within(() => {
                    cy.contains('Select column');
                  })
                  .click();

                cy.get('@body')
                  .find('[aria-label="Select options menu"]')
                  .should('be.visible')
                  .within(() => {
                    const schema = ctx.apiResponse.tablesSchema.schema as Record<string, unknown>;
                    const dimensionFieldSpecs = schema.dimensionFieldSpecs as Array<Record<string, string>>;
                    const metricFieldSpecs = schema.metricFieldSpecs as Array<Record<string, string>>;

                    const selectOptions = [...dimensionFieldSpecs, ...metricFieldSpecs].filter(
                      (item) => item.name !== formData.metricColumn
                    );

                    selectOptions.forEach((option) => cy.contains(option.name));

                    // Close select menu
                    cy.get('@body').click(0, 0);
                  });
              });

            // -- Check query segment operator select --
            cy.get('#query-segment-operator-select')
              .should('exist')
              .within(() => {
                cy.get('input')
                  .should('exist')
                  .parent()
                  .parent()
                  .within(() => {
                    cy.contains('Choose');
                  })
                  .click();

                cy.get('@body')
                  .find('[aria-label="Select options menu"]')
                  .should('be.visible')
                  .within(() => {
                    const selectOptions = ['=', '!=', '>', '>=', '<', '<=', 'like', 'not like'];

                    selectOptions.forEach((option) => cy.contains(option));

                    // Close select menu
                    cy.get('@body').click(0, 0);
                  });
              });

            // -- Check value select --
            cy.get('#value-select')
              .should('exist')
              .within(() => {
                cy.get('input')
                  .should('exist')
                  .parent()
                  .parent()
                  .within(() => {
                    cy.contains('Select value');
                  })
                  .click();

                cy.get('@body')
                  .find('[aria-label="Select options menu"]')
                  .should('be.visible')
                  .within(() => {
                    const selectOptions = ['No options found'];

                    selectOptions.forEach((option) => cy.contains(option));

                    // Close select menu
                    cy.get('@body').click(0, 0);
                  });
              });

            // -- Check filter delete button --
            cy.getBySel('delete-filter-btn').should('exist').click();
            cy.wait(['@dsQuery', '@previewSqlBuilder']);
          });

        // Check filter row should not exits after deleting the row
        cy.getBySel('filter-row').should('not.exist');

        /**
         * Check View filter and it's options
         */
        cy.get('@addFilterBtn').click();
        cy.wait(['@dsQuery', '@previewSqlBuilder']);

        cy.getBySel('filter-row').within(() => {
          // -- Check select column --
          cy.get('#column-select').within(() => {
            cy.get('input').parent().parent().click();

            cy.get('@body')
              .find('[aria-label="Select options menu"]')
              .should('be.visible')
              .within(() => {
                // Select View column filter
                cy.contains('views').click();
                cy.wait(['@dsQuery', '@previewSqlBuilder']);
              });
          });

          // -- Check query segment operator select --
          cy.get('#query-segment-operator-select').within(() => {
            cy.get('input')
              .parent()
              .parent()
              .within(() => {
                cy.contains('=');
              })
              .click();

            cy.get('@body')
              .find('[aria-label="Select options menu"]')
              .should('be.visible')
              .within(() => {
                // Select equals operator
                cy.contains('=').click();
                cy.wait(['@dsQuery', '@previewSqlBuilder']);
              });
          });

          // -- Check value select --
          cy.get('#value-select').within(() => {
            cy.get('input').parent().parent().click();

            cy.wait('@queryDistinctValues').then(({ response }) => {
              const data = response.body as { code: number; valueExprs: string[] };

              cy.get('@body')
                .find('[aria-label="Select options menu"]')
                .should('be.visible')
                .within(() => {
                  // Check the available options
                  data.valueExprs.forEach((valueExpr) => {
                    cy.contains(valueExpr);
                  });

                  // Check the options length should not exceed 100
                  cy.wrap(data.valueExprs).should('have.length', 100);

                  // Close select menu
                  cy.get('@body').click(0, 0);
                });
            });
          });

          // -- Check filter delete button --
          cy.getBySel('delete-filter-btn').click();
          cy.wait(['@dsQuery', '@previewSqlBuilder']);
        });

        /**
         * Add the form data if any
         */
        if (formData.filters && formData.filters.length > 0) {
          formData.filters.forEach((filterOption) => {
            // Add filter row
            cy.get('@addFilterBtn').click();
            cy.wait(['@dsQuery', '@previewSqlBuilder']);

            // -- Check filter row --
            cy.getBySel('filter-row')
              .should('exist')
              .within(() => {
                // Select column
                cy.get('#column-select').within(() => {
                  cy.get('input').parent().parent().as('columnSelect').click();

                  cy.get('@body')
                    .find('[aria-label="Select options menu"]')
                    .should('be.visible')
                    .within(() => {
                      // Select the option
                      cy.contains(filterOption.column).click();

                      cy.wait(['@dsQuery', '@previewSqlBuilder']);
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
                    })
                    .click();

                  cy.get('@body')
                    .find('[aria-label="Select options menu"]')
                    .should('be.visible')
                    .within(() => {
                      // Select the option
                      cy.contains(filterOption.operator).click();

                      cy.wait(['@dsQuery', '@previewSqlBuilder']);
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
                    const data = response.body as { code: number; valueExprs: string[] };

                    cy.get('@body')
                      .find('[aria-label="Select options menu"]')
                      .should('be.visible')
                      .within(() => {
                        // Check the available options
                        data.valueExprs.forEach((valueExpr) => {
                          cy.contains(valueExpr);
                        });

                        // Select the option
                        cy.contains(filterOption.value).click();

                        cy.wait(['@dsQuery', '@previewSqlBuilder']);
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
    cy.getBySel('select-query-options')
      .should('exist')
      .within(() => {
        cy.wrap(cy.$$('body')).as('body');

        // Check form label
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Query Options');

        // Check add query option button
        cy.getBySel('add-query-option-btn').should('exist').as('addQueryOptionBtn').click();
        cy.wait(['@dsQuery', '@previewSqlBuilder']);

        // -- Check query option row --
        cy.getBySel('query-option-row')
          .should('exist')
          .within(() => {
            // Check SET label
            cy.getBySel('set-label').should('exist').and('have.text', 'SET');

            // Check query option select
            cy.get('#query-option-select')
              .should('exist')
              .within(() => {
                cy.get('input')
                  .should('exist')
                  .parent()
                  .parent()
                  .as('queryOptionSelect')
                  .within(() => {
                    cy.contains('Choose');
                  })
                  .click();

                cy.get('@body')
                  .find('[aria-label="Select options menu"]')
                  .should('be.visible')
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
            cy.getBySel('delete-query-option-btn').should('exist').click();
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
            cy.getBySel('query-option-row')
              .should('exist')
              .within(() => {
                // Select the query option
                cy.get('#query-option-select').within(() => {
                  cy.get('input').parent().parent().click();

                  cy.get('@body')
                    .find('[aria-label="Select options menu"]')
                    .should('be.visible')
                    .within(() => {
                      // Select the option
                      cy.contains(queryOption.option).click();

                      cy.wait(['@dsQuery', '@previewSqlBuilder']);
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
     * Check Sql Preview Container
     */
    cy.getBySel('sql-preview-container')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Sql Preview');

        cy.getBySel('sql-preview').should('exist').as('sqlPreview').and('not.be.empty');

        // Check the default limit should be 100000
        cy.get('@sqlPreview').should('contain.text', '100000');

        // Check the copy button in SQL Preview
        cy.get('@sqlPreview').within(() => {
          // Check the Copy button and click
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
     * Check Limit field
     */
    cy.getBySel('input-limit')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Limit');

        cy.get('input').should('exist').and('have.attr', 'placeholder', 'auto').as('limitInput');
      });

    /**
     * Change the limit and check if it's update in SQL Preview
     */
    cy.get('@limitInput').clear().type('1000');
    cy.wait(['@dsQuery', '@previewSqlBuilder']);
    cy.get('@sqlPreview').should('contain.text', '1000');

    /**
     * Clear the limit input and check for default limit value in SQL Preview
     */
    cy.get('@limitInput').clear();
    cy.wait(['@dsQuery', '@previewSqlBuilder']);
    cy.get('@sqlPreview').should('contain.text', '100000');

    /**
     * Fill the limit input from form data
     */
    if (formData.limit != null) {
      cy.get('@limitInput').clear().type(formData.limit.toString());
    }

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

  it('Time series should render when switching from Query Builder to Code Editor', () => {
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
    cy.intercept('GET', '/api/datasources/*/resources/tables/*/schema', (req) => {
      req.continue((res) => (ctx.apiResponse.tablesSchema = res.body));
    }).as('tablesSchema');
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/builder').as('previewSqlBuilder');
    cy.intercept('POST', '/api/datasources/*/resources/query/distinctValues').as('queryDistinctValues');
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/code').as('previewSqlCode');

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
        cy.get('input[aria-label="Time Range from field"]').should('exist').clear().type('2024-06-18 00:00:00');

        // Fill to time field
        cy.get('input[aria-label="Time Range to field"]').should('exist').clear().type('2024-07-18 23:59:59');

        // Apply time range
        cy.get('button').contains('Apply time range').click();
      });

    cy.wait('@dsQuery');

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
      .as('selectEditorMode')
      .within(() => {
        // Check Radio group
        cy.get('input[type="radio"]')
          .eq(0)
          .should('exist')
          .invoke('attr', 'id')
          .then((id) => {
            cy.get(`label[for="${id}"]`).should('exist').and('contain.text', 'Builder').click();
          });
      });

    /**
     * Check Run query button
     */
    cy.getBySel('query-editor-header')
      .should('exist')
      .within(() => {
        cy.getBySel('run-query-btn').should('exist').and('have.text', 'Run Query').as('runQueryBtn');
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
            // Select the option
            cy.contains(formData.table).click();
          });

        // Check if correct option is selected
        cy.get('@tableSelect').within(() => {
          cy.contains(formData.table);
        });
      });

    cy.wait(['@tablesSchema', '@previewSqlBuilder', '@dsQuery']);

    /**
     * Check and select Time Column field
     */
    cy.getBySel('select-time-column')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input').parent().parent().as('timeColumnSelect').click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            // Select the option
            cy.contains(formData.timeColumn).click();
          });

        // Check if correct option is selected
        cy.get('@timeColumnSelect').within(() => {
          cy.contains(formData.timeColumn);
        });
      });

    /**
     * Check and select Granularity field
     */
    cy.getBySel('select-granularity')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input').parent().parent().as('granularitySelect').click();

        cy.wrap(cy.$$('body'))
          .as('body')
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            // Select the option
            if (formData.granularity) {
              cy.contains(formData.granularity).click();

              cy.wait(['@dsQuery', '@previewSqlBuilder']);
            } else {
              // Close the select menu
              cy.get('@body').click(0, 0);
            }
          });

        // Check if correct option is selected
        if (formData.granularity) {
          cy.get('@granularitySelect').within(() => {
            cy.contains(formData.granularity);
          });
        }
      });

    /**
     * Check and select Metric Column field
     */
    cy.getBySel('select-metric-column')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input').parent().parent().as('metricColumnSelect').click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            // Select the option
            cy.contains(formData.metricColumn).click();
            cy.wait(['@dsQuery', '@previewSqlBuilder']);
          });

        // Check if correct option is selected
        cy.get('@metricColumnSelect').within(() => {
          cy.contains(formData.metricColumn);
        });
      });

    /**
     * Check and select Aggregation field
     */
    cy.getBySel('select-aggregation')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input').parent().parent().as('aggregationSelect').click();

        cy.wrap(cy.$$('body'))
          .as('body')
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            // Select the option
            if (formData.aggregation) {
              cy.contains(formData.aggregation).click();

              cy.wait(['@dsQuery']);
            } else {
              // Close the select menu
              cy.get('@body').click(0, 0);
            }
          });

        // Check if correct option is selected
        if (formData.aggregation) {
          cy.get('@aggregationSelect').within(() => {
            cy.contains(formData.aggregation);
          });
        }
      });

    /**
     * Check and select Group By field
     */
    cy.getBySel('select-group-by')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input').parent().parent().as('groupBySelect');

        cy.wrap(cy.$$('body')).as('body');

        // Select the form data options
        if (formData.groupBy && formData.groupBy.length > 0) {
          formData.groupBy.forEach((option) => {
            // Open select menu
            cy.get('@groupBySelect').click();

            cy.get('@body')
              .find('[aria-label="Select options menu"]')
              .should('be.visible')
              .within(() => {
                // Select the option
                cy.contains(option).click();

                cy.wait(['@dsQuery', '@previewSqlBuilder']);
              });

            // Check if option is selected
            cy.get('@groupBySelect').within(() => {
              cy.contains(option);
            });
          });
        }
      });

    /**
     * Check and select Order By field
     */
    cy.getBySel('select-order-by')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input').parent().parent().as('orderBySelect');

        cy.wrap(cy.$$('body')).as('body');

        // Select the form data options
        if (formData.orderBy && formData.orderBy.length > 0) {
          formData.orderBy.forEach((option) => {
            // Open select menu
            cy.get('@orderBySelect').click();

            cy.get('@body')
              .find('[aria-label="Select options menu"]')
              .should('be.visible')
              .within(() => {
                // Select the option
                cy.contains(option).click();

                cy.wait(['@dsQuery', '@previewSqlBuilder']);
              });

            // Check if option is selected
            cy.get('@orderBySelect').within(() => {
              cy.contains(option);
            });
          });
        }
      });

    /**
     * Check and select Filters field
     */
    cy.getBySel('select-filters')
      .should('exist')
      .within(() => {
        cy.wrap(cy.$$('body')).as('body');

        // Check add filter button
        cy.getBySel('add-filter-btn').should('exist').as('addFilterBtn');

        // -- Add the form data if any --
        if (formData.filters && formData.filters.length > 0) {
          formData.filters.forEach((filterOption) => {
            // Add filter row
            cy.get('@addFilterBtn').click();
            cy.wait(['@dsQuery', '@previewSqlBuilder']);

            // -- Check filter row --
            cy.getBySel('filter-row')
              .should('exist')
              .within(() => {
                // Select column
                cy.get('#column-select').within(() => {
                  cy.get('input').parent().parent().as('columnSelect').click();

                  cy.get('@body')
                    .find('[aria-label="Select options menu"]')
                    .should('be.visible')
                    .within(() => {
                      // Select the option
                      cy.contains(filterOption.column).click();

                      cy.wait(['@dsQuery', '@previewSqlBuilder']);
                    });

                  // Check if the option is selected
                  cy.get('@columnSelect').within(() => {
                    cy.contains(filterOption.column);
                  });
                });

                // Select query segment operator
                cy.get('#query-segment-operator-select').within(() => {
                  cy.get('input').parent().parent().as('querySegmentOperatorSelect').click();

                  cy.get('@body')
                    .find('[aria-label="Select options menu"]')
                    .should('be.visible')
                    .within(() => {
                      // Select the option
                      cy.contains(filterOption.operator).click();

                      cy.wait(['@dsQuery']);
                    });

                  // Check if the option is selected
                  cy.get('@querySegmentOperatorSelect').within(() => {
                    cy.contains(filterOption.operator);
                  });
                });

                // Select value
                cy.get('#value-select').within(() => {
                  cy.get('input').parent().parent().as('valueSelect').click();

                  cy.wait('@queryDistinctValues').then(() => {
                    cy.get('@body')
                      .find('[aria-label="Select options menu"]')
                      .should('be.visible')
                      .within(() => {
                        cy.contains(filterOption.value).click();

                        cy.wait(['@dsQuery', '@previewSqlBuilder']);
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
    cy.getBySel('select-query-options')
      .should('exist')
      .within(() => {
        cy.wrap(cy.$$('body')).as('body');

        // Check add query option button
        cy.getBySel('add-query-option-btn').should('exist').as('addQueryOptionBtn');

        // -- Add the form data if any --
        if (formData.queryOptions && formData.queryOptions.length > 0) {
          formData.queryOptions.forEach((queryOption) => {
            // Add query option row
            cy.get('@addQueryOptionBtn').click();
            cy.wait(['@dsQuery', '@previewSqlBuilder']);

            // -- Check query option row --
            cy.getBySel('query-option-row')
              .should('exist')
              .within(() => {
                // Select the query option
                cy.get('#query-option-select').within(() => {
                  cy.get('input').parent().parent().click();

                  cy.get('@body')
                    .find('[aria-label="Select options menu"]')
                    .should('be.visible')
                    .within(() => {
                      // Select the option
                      cy.contains(queryOption.option).click();

                      cy.wait(['@dsQuery', '@previewSqlBuilder']);
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
    cy.getBySel('input-limit')
      .should('exist')
      .within(() => {
        if (formData.limit != null) {
          cy.get('input').should('exist').type(formData.limit.toString());
        }
      });

    /**
     * Check Sql Preview Container
     */
    cy.getBySel('sql-preview-container')
      .should('exist')
      .within(() => {
        cy.getBySel('sql-preview').should('exist').and('not.be.empty');
      });

    /**
     * Check and fill Metric Legend field
     */
    cy.getBySel('metric-legend')
      .should('exist')
      .within(() => {
        if (formData.legend) {
          cy.get('input').should('exist').type(formData.legend);
        }
      });

    /**
     * Finally Run Query and check results
     */
    cy.get('@runQueryBtn').click();
    cy.wait('@dsQuery', { timeout: 5000 }).its('response.body').as('queryBuilderQueryResponse');
    cy.wait('@previewSqlBuilder');

    // Check the UPlot chart for query builder results
    cy.get('.panel-content').should('not.contain', 'No data');
    cy.getBySel('uplot-main-div').should('exist');

    /**
     * Switch to Code Editor
     */
    cy.get('@selectEditorMode').within(() => {
      // Check Radio group
      cy.get('input[type="radio"]')
        .eq(1)
        .should('exist')
        .invoke('attr', 'id')
        .then((id) => {
          cy.get(`label[for="${id}"]`).should('exist').and('contain.text', 'Code').click();
        });
    });

    cy.wait(['@previewSqlBuilder', '@previewSqlCode']);

    /**
     * Check the Code Editor Sql Preview
     */
    cy.getBySel('sql-preview-container')
      .should('exist')
      .within(() => {
        cy.getBySel('sql-preview').should('exist').and('not.be.empty');
      });

    /**
     * Run Query and compare time series results
     */
    cy.get('@runQueryBtn').click();
    cy.wait('@dsQuery', { timeout: 5000 }).then(({ response }) => {
      const codeEditorQueryResponse = response.body;

      cy.get('@queryBuilderQueryResponse').then((queryBuilderQueryResponse) => {
        cy.wrap(codeEditorQueryResponse).should('deep.equal', queryBuilderQueryResponse);
      });
    });

    // Check the UPlot chart for Code Editor results
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

  it('Time series should render when switching from Query Builder to Code Editor to back to Query Builder', () => {
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
    cy.intercept('GET', '/api/datasources/*/resources/tables/*/schema', (req) => {
      req.continue((res) => (ctx.apiResponse.tablesSchema = res.body));
    }).as('tablesSchema');
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/builder').as('previewSqlBuilder');
    cy.intercept('POST', '/api/datasources/*/resources/query/distinctValues').as('queryDistinctValues');
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/code').as('previewSqlCode');

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
        cy.contains('Last 6 months').parent().click();
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
      .as('selectEditorMode')
      .within(() => {
        // Check Builder Radio group
        cy.get('input[type="radio"]')
          .eq(0)
          .should('exist')
          .invoke('attr', 'id')
          .then((id) => {
            cy.get(`label[for="${id}"]`).should('exist').and('contain.text', 'Builder').as('queryBuilderModeTab');
          });

        // Check Code Radio group
        cy.get('input[type="radio"]')
          .eq(1)
          .should('exist')
          .invoke('attr', 'id')
          .then((id) => {
            cy.get(`label[for="${id}"]`).should('exist').and('contain.text', 'Code').as('codeEditorModeTab');
          });

        // Select the query builder tab
        cy.get('@queryBuilderModeTab').click();
      });

    /**
     * Check Run query button
     */
    cy.getBySel('query-editor-header')
      .should('exist')
      .within(() => {
        cy.getBySel('run-query-btn').should('exist').and('have.text', 'Run Query').as('runQueryBtn');
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
            // Select the option
            cy.contains(formData.table).click();
          });

        // Check if correct option is selected
        cy.get('@tableSelect').within(() => {
          cy.contains(formData.table);
        });
      });

    cy.wait(['@tablesSchema', '@previewSqlBuilder', '@dsQuery']);

    /**
     * Check and select Time Column field
     */
    cy.getBySel('select-time-column')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input').parent().parent().as('timeColumnSelect').click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            // Select the option
            cy.contains(formData.timeColumn).click();
          });

        // Check if correct option is selected
        cy.get('@timeColumnSelect').within(() => {
          cy.contains(formData.timeColumn);
        });
      });

    /**
     * Check and select Granularity field
     */
    cy.getBySel('select-granularity')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input').parent().parent().as('granularitySelect').click();

        cy.wrap(cy.$$('body'))
          .as('body')
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            // Select the option
            if (formData.granularity) {
              cy.contains(formData.granularity).click();

              cy.wait(['@dsQuery', '@previewSqlBuilder']);
            } else {
              // Close the select menu
              cy.get('@body').click(0, 0);
            }
          });

        // Check if correct option is selected
        if (formData.granularity) {
          cy.get('@granularitySelect').within(() => {
            cy.contains(formData.granularity);
          });
        }
      });

    /**
     * Check and select Metric Column field
     */
    cy.getBySel('select-metric-column')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input').parent().parent().as('metricColumnSelect').click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            // Select the option
            cy.contains(formData.metricColumn).click();
            cy.wait(['@dsQuery', '@previewSqlBuilder']);
          });

        // Check if correct option is selected
        cy.get('@metricColumnSelect').within(() => {
          cy.contains(formData.metricColumn);
        });
      });

    /**
     * Check and select Aggregation field
     */
    cy.getBySel('select-aggregation')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input').parent().parent().as('aggregationSelect').click();

        cy.wrap(cy.$$('body'))
          .as('body')
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            // Select the option
            if (formData.aggregation) {
              cy.contains(formData.aggregation).click();

              cy.wait(['@dsQuery', '@previewSqlBuilder']);
            } else {
              // Close the select menu
              cy.get('@body').click(0, 0);
            }
          });

        // Check if correct option is selected
        if (formData.aggregation) {
          cy.get('@aggregationSelect').within(() => {
            cy.contains(formData.aggregation);
          });
        }
      });

    /**
     * Check and select Group By field
     */
    cy.getBySel('select-group-by')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input').parent().parent().as('groupBySelect');

        cy.wrap(cy.$$('body')).as('body');

        // Select the form data options
        if (formData.groupBy && formData.groupBy.length > 0) {
          formData.groupBy.forEach((option) => {
            // Open select menu
            cy.get('@groupBySelect').click();

            cy.get('@body')
              .find('[aria-label="Select options menu"]')
              .should('be.visible')
              .within(() => {
                // Select the option
                cy.contains(option).click();

                cy.wait(['@dsQuery', '@previewSqlBuilder']);
              });

            // Check if option is selected
            cy.get('@groupBySelect').within(() => {
              cy.contains(option);
            });
          });
        }
      });

    /**
     * Check and select Order By field
     */
    cy.getBySel('select-order-by')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input').parent().parent().as('orderBySelect');

        cy.wrap(cy.$$('body')).as('body');

        // Select the form data options
        if (formData.orderBy && formData.orderBy.length > 0) {
          formData.orderBy.forEach((option) => {
            // Open select menu
            cy.get('@orderBySelect').click();

            cy.get('@body')
              .find('[aria-label="Select options menu"]')
              .should('be.visible')
              .within(() => {
                // Select the option
                cy.contains(option).click();

                cy.wait(['@dsQuery', '@previewSqlBuilder']);
              });

            // Check if option is selected
            cy.get('@orderBySelect').within(() => {
              cy.contains(option);
            });
          });
        }
      });

    /**
     * Check and select Filters field
     */
    cy.getBySel('select-filters')
      .should('exist')
      .within(() => {
        cy.wrap(cy.$$('body')).as('body');

        // Check add filter button
        cy.getBySel('add-filter-btn').should('exist').as('addFilterBtn');

        // -- Add the form data if any --
        if (formData.filters && formData.filters.length > 0) {
          formData.filters.forEach((filterOption) => {
            // Add filter row
            cy.get('@addFilterBtn').click();
            cy.wait(['@dsQuery', '@previewSqlBuilder']);

            // -- Check filter row --
            cy.getBySel('filter-row')
              .should('exist')
              .within(() => {
                // Select column
                cy.get('#column-select').within(() => {
                  cy.get('input').parent().parent().as('columnSelect').click();

                  cy.get('@body')
                    .find('[aria-label="Select options menu"]')
                    .should('be.visible')
                    .within(() => {
                      // Select the option
                      cy.contains(filterOption.column).click();

                      cy.wait(['@dsQuery', '@previewSqlBuilder']);
                    });

                  // Check if the option is selected
                  cy.get('@columnSelect').within(() => {
                    cy.contains(filterOption.column);
                  });
                });

                // Select query segment operator
                cy.get('#query-segment-operator-select').within(() => {
                  cy.get('input').parent().parent().as('querySegmentOperatorSelect').click();

                  cy.get('@body')
                    .find('[aria-label="Select options menu"]')
                    .should('be.visible')
                    .within(() => {
                      // Select the option
                      cy.contains(filterOption.operator).click();

                      cy.wait(['@dsQuery', '@previewSqlBuilder']);
                    });

                  // Check if the option is selected
                  cy.get('@querySegmentOperatorSelect').within(() => {
                    cy.contains(filterOption.operator);
                  });
                });

                // Select value
                cy.get('#value-select').within(() => {
                  cy.get('input').parent().parent().as('valueSelect').click();

                  cy.wait('@queryDistinctValues').then(() => {
                    cy.get('@body')
                      .find('[aria-label="Select options menu"]')
                      .should('be.visible')
                      .within(() => {
                        cy.contains(filterOption.value).click();

                        cy.wait(['@dsQuery', '@previewSqlBuilder']);
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
    cy.getBySel('select-query-options')
      .should('exist')
      .within(() => {
        cy.wrap(cy.$$('body')).as('body');

        // Check add query option button
        cy.getBySel('add-query-option-btn').should('exist').as('addQueryOptionBtn');

        // -- Add the form data if any --
        if (formData.queryOptions && formData.queryOptions.length > 0) {
          formData.queryOptions.forEach((queryOption) => {
            // Add query option row
            cy.get('@addQueryOptionBtn').click();
            cy.wait(['@dsQuery', '@previewSqlBuilder']);

            // -- Check query option row --
            cy.getBySel('query-option-row')
              .should('exist')
              .within(() => {
                // Select the query option
                cy.get('#query-option-select').within(() => {
                  cy.get('input').parent().parent().click();

                  cy.get('@body')
                    .find('[aria-label="Select options menu"]')
                    .should('be.visible')
                    .within(() => {
                      // Select the option
                      cy.contains(queryOption.option).click();

                      cy.wait(['@dsQuery', '@previewSqlBuilder']);
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
    cy.getBySel('input-limit')
      .should('exist')
      .within(() => {
        if (formData.limit != null) {
          cy.get('input').should('exist').type(formData.limit.toString());
        }
      });

    /**
     * Check Sql Preview Container
     */
    cy.getBySel('sql-preview-container')
      .should('exist')
      .within(() => {
        cy.getBySel('sql-preview').should('exist').and('not.be.empty');
      });

    /**
     * Check and fill Metric Legend field
     */
    cy.getBySel('metric-legend')
      .should('exist')
      .within(() => {
        if (formData.legend) {
          cy.get('input').should('exist').type(formData.legend);
        }
      });

    /**
     * Finally Run Query and check results
     */
    cy.get('@runQueryBtn').click();
    cy.wait('@dsQuery', { timeout: 5000 });
    cy.wait('@previewSqlBuilder');

    // Check the UPlot chart for query builder results
    cy.get('.panel-content').should('not.contain', 'No data');
    cy.getBySel('uplot-main-div').should('exist');

    /**
     * Switch to Code Editor
     */
    cy.get('@codeEditorModeTab').click();
    cy.wait(['@previewSqlBuilder', '@previewSqlCode']);

    /**
     * Check the Code Editor Sql Preview
     */
    cy.getBySel('sql-preview-container')
      .should('exist')
      .within(() => {
        cy.getBySel('sql-preview').should('exist').and('not.be.empty');
      });

    /**
     * Run Query and check results
     */
    cy.get('@runQueryBtn').click();
    cy.wait('@dsQuery', { timeout: 5000 });
    cy.wait('@previewSqlBuilder');

    // Check the UPlot chart for query builder results
    cy.get('.panel-content').should('not.contain', 'No data');
    cy.getBySel('uplot-main-div').should('exist');

    /**
     * Switch back to Query Builder
     */
    cy.get('@queryBuilderModeTab').click();

    /**
     * Check the Confirmation Dialog
     */
    cy.getBySel('modal-header-title')
      .parent()
      .parent()
      .should('be.visible')
      .within(() => {
        cy.getBySel('modal-header-title').should('exist').and('have.text', 'Warning');

        // Check body description text
        cy.getBySel('modal-body')
          .should('exist')
          .within(() => {
            cy.get('p')
              .eq(0)
              .should(
                'have.text',
                'Builder mode does not display changes made in code. The query builder will display the last changes you made in builder mode.'
              );

            cy.get('p').eq(1).should('have.text', 'Do you want to copy your code to the clipboard?');
          });

        // Check Cancel button
        cy.getBySel('cancel-btn').and('have.text', 'Cancel');

        // Check Discard code and switch button
        cy.getBySel('discard-code-and-switch-btn').and('have.text', 'Discard code and switch');

        // Check Copy code and switch button and click
        cy.getBySel('copy-code-and-switch-btn')
          .and('have.text', 'Copy code and switch')
          .click()
          .then(() => {
            cy.window().then((win) => {
              win.focus();
            });
          });
      });

    // Check for dialog close and wait for api calls to finish
    cy.getBySel('modal-header-title').should('not.exist');
    cy.wait(['@tablesSchema', '@previewSqlBuilder']);

    /**
     * Check the Query Builder Sql Preview
     */
    cy.getBySel('sql-preview-container')
      .should('exist')
      .within(() => {
        cy.getBySel('sql-preview').should('exist').and('not.be.empty');
      });

    /**
     * Run Query and check results
     */
    cy.get('@runQueryBtn').click();
    cy.wait('@dsQuery', { timeout: 5000 });
    cy.wait('@previewSqlBuilder');

    // Check the UPlot chart for query builder results
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

  it('Time series should render when selecting only required fields', () => {
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
    cy.intercept('GET', '/api/datasources/*/resources/tables/*/schema', (req) => {
      req.continue((res) => (ctx.apiResponse.tablesSchema = res.body));
    }).as('tablesSchema');
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/builder').as('previewSqlBuilder');
    cy.intercept('POST', '/api/datasources/*/resources/query/distinctValues').as('queryDistinctValues');

    const formData = {
      table: 'complex_website',
      timeColumn: 'hoursSinceEpoch',
      metricColumn: 'views',
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
        cy.contains('Last 6 months').parent().click();
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
          .eq(0)
          .should('exist')
          .invoke('attr', 'id')
          .then((id) => {
            cy.get(`label[for="${id}"]`).should('exist').and('contain.text', 'Builder').click();
          });
      });

    /**
     * Check Run query button
     */
    cy.getBySel('query-editor-header')
      .should('exist')
      .within(() => {
        cy.getBySel('run-query-btn').should('exist').and('have.text', 'Run Query').as('runQueryBtn');
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
            // Select the option
            cy.contains(formData.table).click();
          });

        // Check if correct option is selected
        cy.get('@tableSelect').within(() => {
          cy.contains(formData.table);
        });
      });

    cy.wait(['@tablesSchema', '@previewSqlBuilder', '@dsQuery']);

    /**
     * Check and select Time Column field
     */
    cy.getBySel('select-time-column')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input')
          .parent()
          .parent()
          .as('timeColumnSelect')
          .within(() => {
            // Check already selected option
            cy.contains('hoursSinceEpoch');
          })
          .click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            // Select the option
            cy.contains(formData.timeColumn).click();
          });

        // Check if correct option is selected
        cy.get('@timeColumnSelect').within(() => {
          cy.contains(formData.timeColumn);
        });
      });

    /**
     * Check and select Metric Column field
     */
    cy.getBySel('select-metric-column')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input')
          .parent()
          .parent()
          .as('metricColumnSelect')
          .within(() => {
            // Check already selected option
            cy.contains('clicks');
          })
          .click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            // Select the option
            cy.contains(formData.metricColumn).click();
            cy.wait(['@dsQuery', '@previewSqlBuilder']);
          });

        // Check if correct option is selected
        cy.get('@metricColumnSelect').within(() => {
          cy.contains(formData.metricColumn);
        });
      });

    /**
     * Check Sql Preview Container
     */
    cy.getBySel('sql-preview-container')
      .should('exist')
      .within(() => {
        cy.getBySel('sql-preview').should('exist').and('not.be.empty');
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

  it('Time series should render with every individual aggregation option', () => {
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
    cy.intercept('GET', '/api/datasources/*/resources/tables/*/schema', (req) => {
      req.continue((res) => (ctx.apiResponse.tablesSchema = res.body));
    }).as('tablesSchema');
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/builder').as('previewSqlBuilder');
    cy.intercept('POST', '/api/datasources/*/resources/query/distinctValues').as('queryDistinctValues');

    const formData = {
      table: 'complex_website',
      timeColumn: 'hoursSinceEpoch',
      metricColumn: 'views',
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
        cy.contains('Last 6 months').parent().click();
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
          .eq(0)
          .should('exist')
          .invoke('attr', 'id')
          .then((id) => {
            cy.get(`label[for="${id}"]`).should('exist').and('contain.text', 'Builder').click();
          });
      });

    /**
     * Check Run query button
     */
    cy.getBySel('query-editor-header')
      .should('exist')
      .within(() => {
        cy.getBySel('run-query-btn').should('exist').and('have.text', 'Run Query').as('runQueryBtn');
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
            // Select the option
            cy.contains(formData.table).click();
          });

        // Check if correct option is selected
        cy.get('@tableSelect').within(() => {
          cy.contains(formData.table);
        });
      });

    cy.wait(['@tablesSchema', '@previewSqlBuilder', '@dsQuery']);

    /**
     * Check and select Time Column field
     */
    cy.getBySel('select-time-column')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input')
          .parent()
          .parent()
          .as('timeColumnSelect')
          .within(() => {
            // Check already selected option
            cy.contains('hoursSinceEpoch');
          })
          .click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            // Select the option
            cy.contains(formData.timeColumn).click();
          });

        // Check if correct option is selected
        cy.get('@timeColumnSelect').within(() => {
          cy.contains(formData.timeColumn);
        });
      });

    /**
     * Check and select Metric Column field
     */
    cy.getBySel('select-metric-column')
      .should('exist')
      .within(() => {
        // Check select list options
        cy.get('input')
          .parent()
          .parent()
          .as('metricColumnSelect')
          .within(() => {
            // Check already selected option
            cy.contains('clicks');
          })
          .click();

        cy.wrap(cy.$$('body'))
          .find('[aria-label="Select options menu"]')
          .should('be.visible')
          .within(() => {
            // Select the option
            cy.contains(formData.metricColumn).click();
            cy.wait(['@dsQuery', '@previewSqlBuilder']);
          });

        // Check if correct option is selected
        cy.get('@metricColumnSelect').within(() => {
          cy.contains(formData.metricColumn);
        });
      });

    /**
     * Check Aggregation field
     */
    cy.getBySel('select-aggregation')
      .should('exist')
      .within(() => {
        cy.get('input').parent().parent().as('aggregationSelect');
      });

    /**
     * Check Run Query and results with every Aggregation Option
     */
    ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'NONE'].forEach((option) => {
      // Open select options menu
      cy.get('@aggregationSelect').click();

      cy.get('[aria-label="Select options menu"]')
        .should('be.visible')
        .within(() => {
          // Select the option
          cy.contains(option).click();
          cy.wait(['@dsQuery', '@previewSqlBuilder']);
        });

      // Check if correct option is selected
      cy.get('@aggregationSelect').within(() => {
        cy.contains(option);
      });

      // -- Run Query and check results --
      cy.get('@runQueryBtn').click();
      cy.wait('@dsQuery', { timeout: 5000 });

      // Check the UPlot chart
      cy.get('.panel-content').should('not.contain', 'No data');
      cy.getBySel('uplot-main-div').should('exist');
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

  it('Adding new query with different params should loads separate time series', () => {
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
    cy.intercept('POST', '/api/ds/query', (req) => {
      req.continue((res) => (ctx.apiResponse.dsQuery = res.body));
    }).as('dsQuery');
    cy.intercept('GET', '/api/datasources/*/resources/tables/*/schema', (req) => {
      req.continue((res) => (ctx.apiResponse.tablesSchema = res.body));
    }).as('tablesSchema');
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/builder').as('previewSqlBuilder');
    cy.intercept('POST', '/api/datasources/*/resources/query/distinctValues').as('queryDistinctValues');

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
        cy.contains('Last 6 months').parent().click();
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
     * Create and check new Query with Clicks Metric
     */
    cy.get('[aria-label="Query editor row"]')
      .should('exist')
      .eq(0)
      .within(() => {
        const formData = {
          table: 'complex_website',
          timeColumn: 'hoursSinceEpoch',
          metricColumn: 'clicks',
        };

        cy.wrap(cy.$$('body')).as('body');

        /**
         * Check and Select Editor Mode
         */
        cy.getBySel('select-editor-mode').within(() => {
          // Check Radio group
          cy.get('input[type="radio"]')
            .eq(0)
            .invoke('attr', 'id')
            .then((id) => {
              cy.get(`label[for="${id}"]`).should('exist').and('contain.text', 'Builder').click();
            });
        });

        /**
         * Check Run query button
         */
        cy.getBySel('query-editor-header').within(() => {
          cy.getBySel('run-query-btn').should('have.text', 'Run Query').as('clicksRunQueryBtn');
        });

        /**
         * Check and select Table field
         */
        cy.getBySel('select-table')
          .should('exist')
          .within(() => {
            // Check select list options
            cy.get('input').parent().parent().as('tableSelect').click();

            cy.get('@body')
              .find('[aria-label="Select options menu"]')
              .should('be.visible')
              .within(() => {
                // Select the option
                cy.contains(formData.table).click();
              });

            // Check if correct option is selected
            cy.get('@tableSelect').within(() => {
              cy.contains(formData.table);
            });
          });

        cy.wait(['@tablesSchema', '@previewSqlBuilder', '@dsQuery']);

        /**
         * Check and select Time Column field
         */
        cy.getBySel('select-time-column')
          .should('exist')
          .within(() => {
            // Check select list options
            cy.get('input')
              .parent()
              .parent()
              .as('timeColumnSelect')
              .within(() => {
                // Check already selected option
                cy.contains('hoursSinceEpoch');
              })
              .click();

            cy.get('@body')
              .find('[aria-label="Select options menu"]')
              .should('be.visible')
              .within(() => {
                // Select the option
                cy.contains(formData.timeColumn).click();
              });

            // Check if correct option is selected
            cy.get('@timeColumnSelect').within(() => {
              cy.contains(formData.timeColumn);
            });
          });

        /**
         * Check and select Metric Column field
         */
        cy.getBySel('select-metric-column')
          .should('exist')
          .within(() => {
            // Check select list options
            cy.get('input')
              .parent()
              .parent()
              .as('metricColumnSelect')
              .within(() => {
                // Check already selected option
                cy.contains('clicks');
              })
              .click();

            cy.get('@body')
              .find('[aria-label="Select options menu"]')
              .should('be.visible')
              .within(() => {
                // Select the option
                cy.contains(formData.metricColumn).click();
                cy.wait(['@dsQuery', '@previewSqlBuilder']);
              });

            // Check if correct option is selected
            cy.get('@metricColumnSelect').within(() => {
              cy.contains(formData.metricColumn);
            });
          });

        /**
         * Check Sql Preview Container
         */
        cy.getBySel('sql-preview-container')
          .should('exist')
          .within(() => {
            cy.getBySel('sql-preview').should('exist').and('not.be.empty');
          });

        /**
         * Finally Run Query and check results
         */
        cy.get('@clicksRunQueryBtn').click();
        cy.wait('@dsQuery', { timeout: 5000 }).then(({ response }) => {
          const respData = response.body as any;
          const fields = respData.results.A.frames[0].schema.fields;

          // Check the result data
          cy.wrap(fields[0]).should('have.property', 'name', 'clicks');
          cy.wrap(fields[1]).should('have.property', 'name', 'time');
        });
      });

    /**
     * Check the Time series chart for Clicks metric
     */
    cy.get('.panel-content').should('not.contain', 'No data');
    cy.getBySel('uplot-main-div').should('exist');

    // Check the Clicks chip in the rendered chart
    cy.get('[aria-label="VizLegend series clicks"]').should('exist').and('have.text', 'clicks');

    /**
     * Check Add new Query button and click
     */
    cy.get('button[aria-label="Query editor add query button"]').should('exist').and('contain.text', 'Query').click();

    /**
     * Create and check new Query with Views Metric
     */
    cy.get('[aria-label="Query editor row"]')
      .should('exist')
      .eq(1)
      .within(() => {
        const formData = {
          table: 'complex_website',
          timeColumn: 'hoursSinceEpoch',
          metricColumn: 'views',
        };

        cy.wrap(cy.$$('body')).as('body');

        /**
         * Check and Select Editor Mode
         */
        cy.getBySel('select-editor-mode').within(() => {
          // Check Radio group
          cy.get('input[type="radio"]')
            .eq(0)
            .invoke('attr', 'id')
            .then((id) => {
              cy.get(`label[for="${id}"]`).should('exist').and('contain.text', 'Builder').click();
            });
        });

        /**
         * Check Run query button
         */
        cy.getBySel('query-editor-header').within(() => {
          cy.getBySel('run-query-btn').should('have.text', 'Run Query').as('viewsRunQueryBtn');
        });

        /**
         * Check and select Table field
         */
        cy.getBySel('select-table')
          .should('exist')
          .within(() => {
            // Check select list options
            cy.get('input').parent().parent().as('tableSelect').click();

            cy.get('@body')
              .find('[aria-label="Select options menu"]')
              .should('be.visible')
              .within(() => {
                // Select the option
                cy.contains(formData.table).click();
              });

            // Check if correct option is selected
            cy.get('@tableSelect').within(() => {
              cy.contains(formData.table);
            });
          });

        cy.wait(['@tablesSchema', '@previewSqlBuilder', '@dsQuery']);

        /**
         * Check and select Time Column field
         */
        cy.getBySel('select-time-column')
          .should('exist')
          .within(() => {
            // Check select list options
            cy.get('input')
              .parent()
              .parent()
              .as('timeColumnSelect')
              .within(() => {
                // Check already selected option
                cy.contains('hoursSinceEpoch');
              })
              .click();

            cy.get('@body')
              .find('[aria-label="Select options menu"]')
              .should('be.visible')
              .within(() => {
                // Select the option
                cy.contains(formData.timeColumn).click();
              });

            // Check if correct option is selected
            cy.get('@timeColumnSelect').within(() => {
              cy.contains(formData.timeColumn);
            });
          });

        /**
         * Check and select Metric Column field
         */
        cy.getBySel('select-metric-column')
          .should('exist')
          .within(() => {
            // Check select list options
            cy.get('input')
              .parent()
              .parent()
              .as('metricColumnSelect')
              .within(() => {
                // Check already selected option
                cy.contains('clicks');
              })
              .click();

            cy.get('@body')
              .find('[aria-label="Select options menu"]')
              .should('be.visible')
              .within(() => {
                // Select the option
                cy.contains(formData.metricColumn).click();
                cy.wait(['@dsQuery', '@previewSqlBuilder']);
              });

            // Check if correct option is selected
            cy.get('@metricColumnSelect').within(() => {
              cy.contains(formData.metricColumn);
            });
          });

        /**
         * Check Sql Preview Container
         */
        cy.getBySel('sql-preview-container')
          .should('exist')
          .within(() => {
            cy.getBySel('sql-preview').should('exist').and('not.be.empty');
          });

        /**
         * Finally Run Query and check results
         */
        cy.get('@viewsRunQueryBtn').click();
        cy.wait('@dsQuery', { timeout: 5000 }).then(() => {
          const fields = ctx.apiResponse.dsQuery.results.B.frames[0].schema.fields;

          // Check the result data
          cy.wrap(fields[0]).should('have.property', 'name', 'views');
          cy.wrap(fields[1]).should('have.property', 'name', 'time');
        });
      });

    /**
     * Check the Time series chart for Clicks metric
     */
    cy.get('.panel-content').should('not.contain', 'No data');
    cy.getBySel('uplot-main-div').should('exist');

    // Check the Views chip in the rendered chart
    cy.get('[aria-label="VizLegend series views"]').should('exist').and('have.text', 'views');

    /**
     * Remove the Views Query and check It should not exist in time series chart
     */
    cy.get('button[aria-label="Remove query query operation action"]').should('exist').eq(1).click();

    // Check the length of the Query editor rows
    cy.get('[aria-label="Query editor row"]').should('have.length', 1);

    // Run the Clicks query
    cy.get('@clicksRunQueryBtn').click();
    cy.wait('@dsQuery', { timeout: 5000 });

    // Check the Views Query chip should not exist in the time series chart
    cy.get('[aria-label="VizLegend series views"]').should('not.exist');

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
