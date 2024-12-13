describe('Visualize With Query Builder in Explore', () => {
  beforeEach(() => {
    cy.createPinotDatasource().as('newlyCreatedDatasource');
  });

  afterEach(() => {
    cy.get<{ uid: string }>('@newlyCreatedDatasource').then(({ uid }) => cy.deletePinotDatasource(uid));
  });

  it('Populates the table dropdown', () => {
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) =>
      cy.setupExplore({
        dsName: ds.name,
        queryType: 'PinotQL',
        editorMode: 'Builder',
      })
    );

    cy.wait('@resourcesTables');
    cy.checkDropdown({
      testId: 'select-table',
      wantLabel: 'Table',
      wantSelected: '',
      wantOptions: ['complex_website', 'simple_website'],
    });
  });

  describe('Time column dropdown', () => {
    it('Says no options found before a table is chosen', () => {
      cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) =>
          cy.setupExplore({
            dsName: ds.name,
            queryType: 'PinotQL',
            editorMode: 'Builder',
          })
      );

      cy.checkDropdown({
        testId: 'select-time-column',
        wantLabel: 'Time Column',
        wantOptions: ['No options found'],
      });
    })

    it('Populates the dropdown after a table is chosen', () => {
      cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');
      cy.intercept('POST', '/api/datasources/*/resources/columns').as('resourcesColumns');

      cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) =>
          cy.setupExplore({
            dsName: ds.name,
            queryType: 'PinotQL',
            editorMode: 'Builder',
          })
      );

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
  })



  it('Populates the granularity dropdown', () => {
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');
    cy.intercept('POST', '/api/datasources/*/resources/columns').as('resourcesColumns');
    cy.intercept('POST', '/api/datasources/*/resources/granularities').as('resourcesGranularities');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) =>
      cy.setupExplore({
        dsName: ds.name,
        queryType: 'PinotQL',
        editorMode: 'Builder',
      })
    );

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

  it('Populates the metric column dropdown', () => {
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');
    cy.intercept('POST', '/api/datasources/*/resources/columns').as('resourcesColumns');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) =>
      cy.setupExplore({
        dsName: ds.name,
        queryType: 'PinotQL',
        editorMode: 'Builder',
      })
    );

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

  it('Populates the aggregation dropdown', () => {
    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) =>
      cy.setupExplore({
        dsName: ds.name,
        queryType: 'PinotQL',
        editorMode: 'Builder',
      })
    );
    cy.checkDropdown({
      testId: 'select-aggregation',
      wantLabel: 'Aggregation',
      wantSelected: 'SUM',
      wantOptions: ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'NONE'],
    });
  });

  it('Populates the group by dropdown', () => {
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');
    cy.intercept('POST', '/api/datasources/*/resources/columns').as('resourcesColumns');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) =>
      cy.setupExplore({
        dsName: ds.name,
        queryType: 'PinotQL',
        editorMode: 'Builder',
      })
    );

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

  it('Populates the order by dropdown', () => {
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');
    cy.intercept('POST', '/api/datasources/*/resources/columns').as('resourcesColumns');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) =>
      cy.setupExplore({
        dsName: ds.name,
        queryType: 'PinotQL',
        editorMode: 'Builder',
      })
    );

    cy.wait('@resourcesTables');
    cy.selectFromDropdown({ testId: 'select-table', value: 'complex_website' });

    cy.wait('@resourcesColumns');
    cy.selectFromDropdown({ testId: 'select-metric-column', value: 'clicks' });
    cy.selectFromDropdown({ testId: 'select-group-by', value: 'country' });
    cy.selectFromDropdown({ testId: 'select-group-by', value: 'browser' });

    cy.checkDropdown({
      testId: 'select-order-by',
      wantLabel: 'Order By',
      wantOptions: ['country asc', 'country desc', 'browser asc', 'browser desc'],
    });
  });

  it('Populates all dropdowns for the query options editor', () => {
    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) =>
      cy.setupExplore({
        dsName: ds.name,
        queryType: 'PinotQL',
        editorMode: 'Builder',
      })
    );

    cy.getBySel('select-query-options').should('exist');
    cy.getBySel('select-query-options').within(() => {
      cy.wrap(cy.$$('body')).as('body');

      // Check form label
      cy.checkFormLabel({ wantLabel: 'Query Options' });

      // Check add query option button
      cy.getBySel('add-query-option-btn').should('exist').as('addQueryOptionBtn');
      cy.getBySel('add-query-option-btn').click();

      // -- Check query option row --
      cy.getBySel('query-option-row').should('exist');
      cy.getBySel('query-option-row').within(() => {
        cy.getBySel('set-label').should('exist').and('have.text', 'SET');

        // Check query option select
        cy.checkDropdown({
          testId: 'query-option-select-name',
          wantOptions: [
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
          ],
        });

        cy.getBySel('operator-label').should('exist').and('have.text', '=');

        cy.checkTextField({ testId: 'query-option-value-input', wantContent: '' });

        cy.getBySel('delete-query-option-btn').should('exist');
        cy.getBySel('delete-query-option-btn').click({ force: true });
      });
      cy.getBySel('query-option-row').should('not.exist');
    });
  });

  it('Populates all dropdowns for the filter editor', () => {
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');
    cy.intercept('POST', '/api/datasources/*/resources/columns').as('resourcesColumns');
    cy.intercept('POST', '/api/datasources/*/resources/query/distinctValues').as('queryDistinctValues');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) =>
      cy.setupExplore({
        dsName: ds.name,
        queryType: 'PinotQL',
        editorMode: 'Builder',
      })
    );

    cy.wait('@resourcesTables');
    cy.selectFromDropdown({ testId: 'select-table', value: 'complex_website' });

    cy.wait('@resourcesColumns');
    cy.selectFromDropdown({ testId: 'select-time-column', value: 'hoursSinceEpoch' });

    cy.getBySel('select-filters').should('exist');
    cy.getBySel('select-filters').within(() => {
      cy.wrap(cy.$$('body')).as('body');
      cy.checkFormLabel({ wantLabel: 'Filters' });

      cy.getBySel('add-filter-btn').should('exist').as('addFilterBtn');
      cy.getBySel('add-filter-btn').click();

      cy.getBySel('filter-row').should('exist');
      cy.getBySel('filter-row').within(() => {
        cy.checkDropdown({ testId: 'query-filter-column-select', wantOptions: [`country`, `browser`] });
        cy.selectFromDropdown({ testId: 'query-filter-column-select', value: `country` });

        cy.checkDropdown({
          testId: 'query-filter-operator-select',
          wantOptions: ['=', '!=', '>', '>=', '<', '<=', 'like', 'not like'],
        });
        cy.selectFromDropdown({ testId: 'query-filter-operator-select', value: '!=' });

        cy.checkDropdown({
          testId: 'query-filter-value-select',
          wantOptions: [`'US'`, `'CN'`],
        });
        cy.selectFromDropdown({ testId: 'query-filter-value-select', value: `'US'` });

        cy.getBySel('delete-filter-btn').should('exist');
        cy.getBySel('delete-filter-btn').click();
      });

      cy.getBySel('filter-row').should('not.exist');
    });
  });

  it('Renders graph and table with minimum fields', () => {
    cy.intercept('POST', '/api/ds/query').as('dsQuery');
    cy.intercept('POST', '/api/datasources/*/resources/columns').as('columns');
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/builder').as('previewSqlBuilder');
    cy.intercept('POST', '/api/datasources/*/resources/query/distinctValues').as('queryDistinctValues');
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) =>
      cy.setupExplore({
        dsName: ds.name,
        queryType: 'PinotQL',
        editorMode: 'Builder',
      })
    );

    cy.wait('@resourcesTables');
    cy.selectFromDropdown({ testId: 'select-table', value: 'complex_website' });

    cy.wait(['@columns', '@previewSqlBuilder', '@dsQuery']);
    cy.pinotQlBuilder_CheckSqlPreview();

    cy.getBySel('explore-no-data').should('not.exist');
    cy.get('div').contains('Graph').should('exist');
    cy.get('[aria-label="Explore Table"]').should('exist');
  });

  it('Renders graph and table with all fields', () => {
    cy.intercept('POST', '/api/ds/query').as('dsQuery');
    cy.intercept('POST', '/api/datasources/*/resources/columns').as('columns');
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/builder').as('previewSqlBuilder');
    cy.intercept('POST', '/api/datasources/*/resources/granularities').as('resourcesGranularities');
    cy.intercept('POST', '/api/datasources/*/resources/query/distinctValues').as('queryDistinctValues');
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) =>
      cy.setupExplore({
        dsName: ds.name,
        queryType: 'PinotQL',
        editorMode: 'Builder',
      })
    );

    cy.wait('@resourcesTables');
    cy.selectFromDropdown({ testId: 'select-table', value: 'complex_website' });

    cy.wait(['@columns', '@previewSqlBuilder', '@dsQuery']);

    cy.selectFromDropdown({ testId: 'select-time-column', value: 'hoursSinceEpoch' });

    cy.wait('@resourcesGranularities');
    cy.selectFromDropdown({ testId: 'select-granularity', value: 'HOURS' });
    cy.wait(['@dsQuery', '@previewSqlBuilder']);

    cy.selectFromDropdown({ testId: 'select-metric-column', value: 'clicks' });
    cy.wait(['@dsQuery', '@previewSqlBuilder']);

    cy.selectFromDropdown({ testId: 'select-aggregation', value: 'AVG' });
    cy.wait(['@dsQuery', '@previewSqlBuilder']);

    cy.selectFromDropdown({ testId: 'select-group-by', value: 'country' });
    cy.wait(['@dsQuery', '@previewSqlBuilder']);

    cy.selectFromDropdown({ testId: 'select-group-by', value: 'browser' });
    cy.wait(['@dsQuery', '@previewSqlBuilder']);

    cy.selectFromDropdown({ testId: 'select-order-by', value: 'browser asc' });
    cy.wait(['@dsQuery', '@previewSqlBuilder']);

    cy.selectFromDropdown({ testId: 'select-order-by', value: 'country asc' });
    cy.wait(['@dsQuery', '@previewSqlBuilder']);

    cy.pinotQlBuilder_AddQueryFilter({ columnName: 'country', operator: '!=', values: ['US'] });
    cy.pinotQlBuilder_AddQueryOptions({ name: 'timeoutMs', value: '1000' });

    cy.checkTextField({ testId: 'input-limit', wantLabel: 'Limit', wantContent: '', wantPlaceholder: 'auto' });
    cy.fillTextField({ testId: 'input-limit', content: '100' });

    cy.checkTextField({ testId: 'metric-legend', wantLabel: 'Legend', wantContent: '' });
    cy.fillTextField({ testId: 'metric-legend', content: 'legend' });

    cy.pinotQlBuilder_CheckSqlPreview(`SET timeoutMs=1000;

SELECT
    "country",
    "browser",
    DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS') AS "time",
    AVG("clicks") AS "metric"
FROM
    "complex_website"
WHERE
    "hoursSinceEpoch" >= 475540 AND "hoursSinceEpoch" < 479932
    AND ("country" != 'US')
GROUP BY
    "country",
    "browser",
    DATETIMECONVERT("hoursSinceEpoch", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')
ORDER BY
    "browser" ASC,
    "country" ASC
LIMIT 100;`);

    cy.getBySel('query-editor-header').should('exist');
    cy.getBySel('query-editor-header').within(() => {
      cy.getBySel('run-query-btn').should('exist').and('have.text', 'Run Query');
      cy.getBySel('run-query-btn').click();
    });

    cy.wait('@dsQuery', { timeout: 5000 });
    cy.wait('@previewSqlBuilder');

    cy.getBySel('explore-no-data').should('not.exist');

    cy.get('div').contains('Graph').should('exist');
    cy.get('[aria-label="Explore Table"]').should('exist');
  });
});
