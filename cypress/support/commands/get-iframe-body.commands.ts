// -- Get Iframe Body custom command --
Cypress.Commands.add('getIframeBody', (iframeSelector) => {
  return cy
    .get(`iframe${iframeSelector}`)
    .its('0.contentDocument')
    .should('exist')
    .its('body')
    .should('not.be.undefined')
    .then(cy.wrap);
});
