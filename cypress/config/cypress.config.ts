import { defineConfig } from 'cypress';
import { config as dotenvConfig } from 'dotenv';
import type { CypressConfigOptions } from './config.interfaces';

dotenvConfig({ path: '../.env' });

const config: CypressConfigOptions = {
  experimentalMemoryManagement: true,
  retries: {
    runMode: 2,
  },
  e2e: {
    baseUrl: 'http://localhost:3000',
    env: {
      // Auth credentials
      authUsername: process.env.AUTH_USERNAME,
      authPassword: process.env.AUTH_PASSWORD,
      authAccessToken: process.env.AUTH_ACCESS_TOKEN,

      // Pinot connection credentials
      pinotConnectionControllerUrl: process.env.PINOT_CONNECTION_CONTROLLER_URL,
      pinotConnectionBrokerUrl: process.env.PINOT_CONNECTION_BROKER_URL,
      pinotConnectionDatabase: process.env.PINOT_CONNECTION_DATABASE,
      pinotConnectionAuthToken: process.env.PINOT_CONNECTION_AUTH_TOKEN,
    },
    setupNodeEvents(on, config) {
      // It's IMPORTANT to return the config object
      // with any changed environment variables
      return config;
    },
  },
};

export default defineConfig(config);
