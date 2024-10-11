// eslint-disable-next-line spaced-comment
/// <reference types="cypress" />

declare namespace Cypress {
  interface Chainable {
    // Overwrite commands
    type(
      element: JQuery<HTMLElement>,
      url: string,
      options?: Partial<Cypress.TypeOptions>
    ): Cypress.Chainable<JQuery<HTMLElement>>;

    // Visit DM url
    visitDmUrl(url: string, options?: Partial<Cypress.VisitOptions>): Cypress.Chainable<Cypress.AUTWindow>;

    // Get Iframe body
    getIframeBody(iframeSelector: string): Chainable<unknown>;

    // Selectors
    getBySel(
      dataTestAttribute: string,
      options?: Partial<Loggable & Timeoutable & Withinable & Shadow>
    ): Chainable<JQuery<HTMLElement>>;
    getBySelLike(
      dataTestPrefixAttribute: string,
      options?: Partial<Loggable & Timeoutable & Withinable & Shadow>
    ): Chainable<JQuery<HTMLElement>>;

    // Delete Pinot Data Source
    deletePinotDatasource(uid: string): Chainable<void>;
  }
}

// For overriding properties type in original object
type Override<Type, NewType extends { [key in keyof Type]?: NewType[key] }> = Omit<Type, keyof NewType> & NewType;
