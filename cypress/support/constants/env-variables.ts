import type { EndToEndEnvVariables } from 'config/config.interfaces';

export const EnvVariables: EndToEndEnvVariables = {
  // Auth
  authUsername: 'authUsername',
  authPassword: 'authPassword',
  authAccessToken: 'authAccessToken',

  // Pinot connection credentials
  pinotConnectionControllerUrl: 'pinotConnectionControllerUrl',
  pinotConnectionBrokerUrl: 'pinotConnectionBrokerUrl',
  pinotConnectionDatabase: 'pinotConnectionDatabase',
  pinotConnectionAuthToken: 'pinotConnectionAuthToken',
};
