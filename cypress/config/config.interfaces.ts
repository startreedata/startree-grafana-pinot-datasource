export type EndToEndEnvVariables = {
  // Auth
  authUsername: string;
  authPassword: string;
  authAccessToken: string;
};

type EndToEnd = Override<
  Cypress.EndToEndConfigOptions,
  {
    env: EndToEndEnvVariables;
  }
>;

export type CypressConfigOptions = Override<
  Cypress.ConfigOptions,
  {
    e2e: EndToEnd;
  }
>;
