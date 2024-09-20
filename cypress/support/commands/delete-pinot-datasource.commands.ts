Cypress.Commands.add('deletePinotDatasource', (uid) => {
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
