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

    createPinotDatasource(): Cypress.Chainable<{ name: string; uid: string }>;

    deletePinotDatasourceWithUi(uid: string): Cypress.Chainable<{ success: boolean }>;

    deletePinotDatasource(uid: string): Cypress.Chainable<void>;

    selectDatasource(name: string): Cypress.Chainable<void>;

    setDashboardTimeRange(timeRange: { from: string; to: string }): Cypress.Chainable<void>;

    checkDropdown(params: {
      testId: string;
      wantLabel?: string;
      wantSelected?: string;
      wantOptions?: string[];
    }): Cypress.Chainable<void>;

    selectFromDropdown(params: { testId: string; value: string }): Cypress.Chainable<void>;

    pinotQlBuilder_CheckSqlPreview(want?: string): Cypress.Chainable<void>;

    fillTextField(params: { testId: string; content: string }): Cypress.Chainable<void>;

    checkTextField(params: {
      testId: string;
      wantLabel?: string;
      wantContent?: string;
      wantPlaceholder?: string;
    }): Cypress.Chainable<void>;

    checkFormLabel(params: { wantLabel: string }): Cypress.Chainable<void>;

    pinotQlBuilder_AddQueryOptions(params: { name: string; value: string }): Cypress.Chainable<void>;

    pinotQlBuilder_AddQueryFilter(params: {
      columnName: string;
      operator: string;
      values: string[];
    }): Cypress.Chainable<void>;

    setupExplore(params: {
      dsName: string;
      queryType: 'PinotQL' | 'PromQL';
      editorMode: 'Builder' | 'Code';
    }): Cypress.Chainable<void>;

    checkRadio(params: { testId: string; wantLabel?: string; wantOptions?: string[] }): Cypress.Chainable<void>;

    selectFromRadio(params: { testId: string; option: string }): Cypress.Chainable<void>;

    checkSqlEditor(params: { wantContent?: string }): Cypress.Chainable<void>;

    fillSqlEditor(params: { content: string }): Cypress.Chainable<void>;

    clickRunQuery(): Cypress.Chainable<void>;
  }
}

// For overriding properties type in original object
type Override<Type, NewType extends { [key in keyof Type]?: NewType[key] }> = Omit<Type, keyof NewType> & NewType;
