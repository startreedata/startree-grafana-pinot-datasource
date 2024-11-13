Cypress.Commands.add('deletePinotDatasource', (uid: string) => {
  cy.request({
    method: 'DELETE',
    url: `/api/datasources/uid/${uid}`,
    headers: {},
    failOnStatusCode: false,
  }).then((response) => {
    if (response.status >= 500) {
      cy.log('Error on deleting pinot data source');
    }

    return;
  });
});

Cypress.Commands.add('deletePinotDatasourceWithUi', (uid: string): Cypress.Chainable<{ success: boolean }> => {
  cy.visit(`/datasources/edit/${uid}`);
  cy.location('pathname').should('eq', `/datasources/edit/${uid}`);

  cy.wait(['@datasourcesUidAccessControl', '@pluginsSettings']);

  // Check for Delete button and click
  cy.get('button').contains('Delete').click();
  cy.get('[data-overlay-container="true"]').within(() => {
    cy.get('button').contains('Delete').click();
  });

  // Wait for delete request to complete
  return cy.wait('@deleteDatasource').then(({ response }) => {
    // Check for success alert and pathname
    cy.get('[data-testid="data-testid Alert success"]').contains('Data source deleted').should('be.visible');
    cy.wait(['@frontendSettings']);
    cy.location('pathname').should('eq', '/datasources');

    return cy.wrap({ success: response.statusCode === 200 });
  });
});
