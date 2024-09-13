export type EndToEndEnvVariables = {
  // Auth
  authUsername: string;
  authPassword: string;
  authAccessToken: string;

  // Pinot connection credentials
  pinotConnectionControllerUrl: string;
  pinotConnectionBrokerUrl: string;
  pinotConnectionDatabase: string;
  pinotConnectionAuthToken: string;
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
