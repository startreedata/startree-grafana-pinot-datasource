import type { EndToEndEnvVariables } from 'config/config.interfaces';

export const EnvVariables: EndToEndEnvVariables = {
  // Pinot connection credentials
  pinotConnectionControllerUrl: 'pinotConnectionControllerUrl',
  pinotConnectionBrokerUrl: 'pinotConnectionBrokerUrl',
  pinotConnectionDatabase: 'pinotConnectionDatabase',
  pinotConnectionAuthToken: 'pinotConnectionAuthToken',
};
