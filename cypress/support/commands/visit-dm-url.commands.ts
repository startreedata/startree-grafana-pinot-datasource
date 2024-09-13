// -- Visit data manager page custom command --
Cypress.Commands.add('visitDmUrl', (url, options) => {
  // cy.setAuthTokenHeader();

  return cy.visit(url, options).then(() => {
    cy.wait(['@apiInfo', '@envInfo'], { timeout: 20000 });
  });
});
