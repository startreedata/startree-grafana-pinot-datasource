# Pinot Data Source plugin for Grafana

The Pinot Data Source Plugin for Grafana allows you to create visualizations in Grafana from data in your Pinot cluster.

**System Requirements**

Grafana version >= 9.1.1

## Development

### Backend

1. Build backend plugin binaries for Linux, Windows and Darwin:

```bash
mage -v
```

2. List all available Mage targets for additional commands:

```bash
mage -l
```

### Frontend

1. Install dependencies

```bash
yarn install
```

2. Build plugin in development mode and run in watch mode

```bash
npm run dev
```

3. Build plugin in production mode

```bash
npm run build
```

4. Run the tests (using Jest)

```bash
# Runs the tests and watches for changes, requires git init first
npm run test

# Exits after running all the tests
npm run test:ci
```

5. Spin up a Grafana instance and run the plugin inside it (using Docker)

```bash
npm run server
```

6. Run the E2E tests (using Cypress)

```bash
# Spins up a Grafana instance first that we tests against
npm run server

# Starts the tests
npm run e2e
```

7. Run the linter

```bash
npm run lint

# or

npm run lint:fix
```

## Release

New releases should be tested and approved by [#observability](https://startreedata.slack.com/archives/C06LUQ8UYD6).

Pushing a new version tag should automatically trigger a new unsigned release build.

Due to plugin signing restrictions, we have to create a release artifact for each intended grafana instance. These _installable_ releases are signed copies of the unsigned release and only valid for the intended grafana instance.

[Create new signed releases.](https://github.com/startreedata/startree-grafana-pinot-datasource/actions/workflows/customer-release.yml)

### Push a version tag

1. Run `npm version <major|minor|patch>`
2. Run `git push origin main --follow-tags`

