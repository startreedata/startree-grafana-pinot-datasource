import { createPinotDatasource, deletePinotDatasource } from './create-pinot-datasource';

export interface TestCtx {
  newlyCreatedDatasourceUid: null | string;
  apiResponse: {
    resourcesTables?: Record<string, unknown>;
    tablesSchema?: Record<string, unknown>;
  };
}

describe('Create and run pinot query using Explore', () => {
  const ctx: TestCtx = {
    newlyCreatedDatasourceUid: null,
    apiResponse: {},
  };

  afterEach(() => {
    // Delete newly created data source after tests
    if (ctx.newlyCreatedDatasourceUid) {
      cy.deletePinotDatasource(ctx.newlyCreatedDatasourceUid);
    }
  });

  it('Graph and Table should rendered using Pinot Query Builder', () => {
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
    cy.visit('/explore');
    cy.location('pathname').should('eq', '/explore');
    cy.wait('@resourcesTables');

    /**
     * Check and select Data source
     */
    cy.get('@newlyCreatedDatasource').then((data: unknown) => {
      const pinotDatasourceName: string = (data as any).name;

      cy.get('#data-source-picker').should('be.visible').parent().parent().as('dataSourcePicker').click();
      cy.get('[aria-label="Select options menu"]')
        .should('be.visible')
        .within(() => {
          cy.contains(pinotDatasourceName).click();
        });

      // Check the selected data source
      cy.get('@dataSourcePicker').should('contain.text', pinotDatasourceName);
    });

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
     * Check if No data message for graph and table is exist
     */
    cy.getBySel('explore-no-data').should('exist').and('have.text', 'No data');

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
            cy.get('label').eq(0).should('exist').and('contain.text', 'PinotQL');
            cy.get('label').eq(1).should('exist').and('contain.text', 'PromQL');
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
              cy.wait('@dsQuery');
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
            cy.wait('@dsQuery');
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
              cy.wait('@dsQuery');
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
            cy.wait('@dsQuery');

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
                cy.wait('@dsQuery');
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
          .scrollIntoView()
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
            cy.contains(selectOptions[0]).click({ force: true });
            cy.wait('@dsQuery');

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
                cy.wait('@dsQuery');
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

        // -- Check filter row --
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
                    })
                    .click();

                  cy.get('@body')
                    .find('[aria-label="Select options menu"]')
                    .should('be.visible')
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
                    const data = response.body as { code: number; valueExprs: string[] };
                    cy.log('data: ', data);

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
    cy.getBySel('input-limit')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Limit');

        cy.get('input').should('exist').and('have.attr', 'placeholder', 'auto').as('limitInput');

        if (formData.limit != null) {
          cy.get('@limitInput').type(formData.limit.toString());
        }
      });

    /**
     * Check Sql Preview Container
     */
    cy.getBySel('sql-preview-container')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Sql Preview');

        cy.getBySel('sql-preview').should('exist').and('not.be.empty');
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
    cy.wait('@previewSqlBuilder');

    // Check the No data message for graph and table should not exist
    cy.getBySel('explore-no-data').should('not.exist');

    // Check the Graph div
    cy.get('div').contains('Graph').should('exist');

    // Check the Table div
    cy.get('[aria-label="Explore Table"]').should('exist');

    /**
     * Delete the newly created data source for the panel
     */
    cy.get('@newlyCreatedDatasource').then((data: unknown) => {
      const datasourceUid = (data as any).uid;
      deletePinotDatasource(ctx, datasourceUid);
    });
  });

  it('Graph and Table should rendered using Pinot Code Editor', () => {
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
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/code').as('previewSqlCode');

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
     * Create new Pinot Datasource for testing explore flow
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
    cy.visit('/explore');
    cy.location('pathname').should('eq', '/explore');
    cy.wait('@resourcesTables');

    /**
     * Check and select Data source
     */
    cy.get('@newlyCreatedDatasource').then((data: unknown) => {
      const pinotDatasourceName: string = (data as any).name;

      cy.get('#data-source-picker').should('be.visible').parent().parent().as('dataSourcePicker').click();
      cy.get('[aria-label="Select options menu"]')
        .should('be.visible')
        .within(() => {
          cy.contains(pinotDatasourceName).click();
        });

      // Check the selected data source
      cy.get('@dataSourcePicker').should('contain.text', pinotDatasourceName);
    });

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
     * Check if No data message for graph and table is exist
     */
    cy.getBySel('explore-no-data').should('exist').and('have.text', 'No data');

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
            cy.get('label').eq(0).should('exist').and('contain.text', 'PinotQL');
            cy.get('label').eq(1).should('exist').and('contain.text', 'PromQL');
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
     * Check SQL Preview
     */
    cy.getBySel('sql-preview-container')
      .should('exist')
      .within(() => {
        cy.getBySel('inline-form-label').should('exist').and('have.text', 'Sql Preview');

        cy.getBySel('sql-preview')
          .should('exist')
          .and('not.be.empty')
          .as('sqlPreview')
          .within(() => {
            // Check the Copy button and click
            cy.getBySel('copy-query-btn').should('exist');
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
    cy.wait('@previewSqlCode');

    // Check the No data message for graph and table should not exist
    cy.getBySel('explore-no-data').should('not.exist');

    // Check the Graph div
    cy.get('div').contains('Graph').should('exist');

    // Check the Table div
    cy.get('[aria-label="Explore Table"]').should('exist');

    /**
     * Delete the newly created data source for the explore
     */
    cy.get('@newlyCreatedDatasource').then((data: unknown) => {
      const datasourceUid = (data as any).uid;
      deletePinotDatasource(ctx, datasourceUid);
    });
  });
});
