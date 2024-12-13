describe('Visualize With Code Editor in Explore', () => {
  beforeEach(() => {
    cy.createPinotDatasource().as('newlyCreatedDatasource');
  });

  afterEach(() => {
    cy.get<{ uid: string }>('@newlyCreatedDatasource').then(({ uid }) => cy.deletePinotDatasource(uid));
  });

  it('Renders graph and table', () => {
    /**
     * All Intercepts
     */
    cy.intercept('POST', '/api/ds/query').as('dsQuery');
    cy.intercept('GET', '/api/datasources/*/resources/tables').as('resourcesTables');
    cy.intercept('POST', '/api/datasources/*/resources/preview/sql/code').as('previewSqlCode');

    cy.get<{ name: string }>('@newlyCreatedDatasource').then((ds) =>
      cy.setupExplore({
        dsName: ds.name,
        queryType: 'PinotQL',
        editorMode: 'Code',
      })
    );

    cy.wait('@resourcesTables');
    cy.checkDropdown({
      testId: 'select-table',
      wantLabel: 'Table',
      wantOptions: ['complex_website', 'simple_website'],
    });
    cy.selectFromDropdown({ testId: 'select-table', value: 'complex_website' });

    cy.checkTextField({ testId: 'time-column-alias', wantLabel: 'Time Alias', wantPlaceholder: 'time' });
    cy.fillTextField({ testId: 'time-column-alias', content: 'time_alias' });

    cy.checkTextField({ testId: 'metric-column-alias', wantLabel: 'Metric Alias', wantPlaceholder: 'metric' });
    cy.fillTextField({ testId: 'metric-column-alias', content: 'metric_alias' });

    cy.checkSqlEditor({
      // language=text
      wantContent: `SELECT $__timeGroup("timestamp") AS $__timeAlias(), SUM("metric") AS $__metricAlias()
FROM $__table()
WHERE $__timeFilter("timestamp")
GROUP BY $__timeGroup("timestamp")
ORDER BY $__timeAlias() DESC
LIMIT 100000`,
    });
    cy.fillSqlEditor({
      // language=text
      content: `SELECT
    $__timeGroup("hoursSinceEpoch") AS $__timeAlias(),
    SUM("views") AS $__metricAlias()
FROM $__table()
WHERE $__timeFilter("hoursSinceEpoch")
GROUP BY $__timeGroup("hoursSinceEpoch")
ORDER BY $__timeAlias() DESC
LIMIT 1000`,
    });

    cy.checkTextField({ testId: 'metric-legend', wantLabel: 'Legend' });
    cy.fillTextField({ testId: 'metric-legend', content: 'sum(views)' });

    cy.clickRunQuery();
    cy.wait('@dsQuery', { timeout: 5000 });
    cy.wait('@previewSqlCode');

    cy.getBySel('explore-no-data').should('not.exist');
    cy.get('div').contains('Graph').should('exist');
    cy.get('[aria-label="Explore Table"]').should('exist');
  });
});
