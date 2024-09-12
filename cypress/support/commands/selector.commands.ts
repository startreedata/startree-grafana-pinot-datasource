Cypress.Commands.add('getBySel', (selector, options) => {
  return cy.get(`[data-testid=${selector}]`, options);
});

Cypress.Commands.add('getBySelLike', (selector, options) => {
  return cy.get(`[data-test*=${selector}]`, options);
});
