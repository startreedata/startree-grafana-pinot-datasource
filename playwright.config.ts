import type { PluginOptions } from '@grafana/plugin-e2e';
import { defineConfig, devices } from '@playwright/test';
import { dirname } from 'node:path';

const pluginE2eAuth = `${dirname(require.resolve('@grafana/plugin-e2e'))}/auth`;

// https://github.com/motdotla/dotenv
require('dotenv').config({ path: './tests/.env' });

// https://playwright.dev/docs/test-configuration.
export default defineConfig<PluginOptions>({
  testDir: './tests',
  /* Run tests in files in parallel */
  fullyParallel: true,
  /* Fail the build on CI if you accidentally left test.only in the source code. */
  forbidOnly: !!process.env.CI,
  /* The heaviest specs (many dashboard-variable interactions) can exceed Playwright's 30s
   * default under CI load; give them headroom. */
  timeout: process.env.CI ? 60_000 : 30_000,
  /* Retry on CI only */
  retries: process.env.CI ? 2 : 0,
  /* Parallelize on CI: Pinot now runs locally in the job (dedicated, not a shared remote
   * cluster), so multiple workers no longer overload it. 1 worker couldn't finish the suite
   * within the step timeout; 2 keeps it well under the cap while avoiding the click/typing
   * races that 4 parallel browser sessions provoke against the single local Grafana. */
  workers: process.env.CI ? 2 : undefined,
  /* Reporter to use. See https://playwright.dev/docs/test-reporters */
  reporter: 'html',
  /* Shared settings for all the projects below. See https://playwright.dev/docs/api/class-testoptions. */
  use: {
    /* Base URL to use in actions like `await page.goto('/')`. */
    baseURL: 'http://localhost:3000',

    /* Collect trace when retrying the failed test. See https://playwright.dev/docs/trace-viewer */
    trace: 'retain-on-failure',
    
    /* Record video on failure */
    video: 'retain-on-failure',
    
    /* Take screenshot on failure */
    screenshot: 'only-on-failure',
  },

  /* Configure projects for major browsers */
  projects: [
    // 1. Login to Grafana and store the cookie on disk for use in other tests.
    {
      name: 'auth',
      testDir: pluginE2eAuth,
      testMatch: [/.*\.js/],
    },
    // 2. Run tests in Google Chrome. Every test will start authenticated as admin user.
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'], storageState: 'playwright/.auth/admin.json' },
      dependencies: ['auth'],
    },
  ],
});
