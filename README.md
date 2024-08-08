# Pinot Data Source plugin for Grafana

The Pinot Data Source Plugin for Grafana allows you to create visualizations in Grafana from data in your Pinot cluster.

**System Requirements**

Grafana version >= 9.1.1

## Development

I typically use two terminal for development: one to run the grafana container and one to run the ui hotloader

Build the frontend and backend and launch the grafana container:

```shell
 npm run build && mage -v && npm run server
```

Launch the hotloader:

```shell
npm run dev
```

### Dev Dependencies

1. Install Docker | [Installation instructions](https://docs.docker.com/desktop/install/mac-install/).

2. Install Mage

```shell
brew install mage
```

3. Install nvm

```shell
brew install nvm
```

4. Install node

```shell
nvm install 20
```

### Backend

Build backend plugin binaries for Linux, Windows and Darwin:

```bash
mage -v
```

Run backend tests:

```bash
docker compose up pinot --detach --wait --wait-timeout 500
go test ./... -v
```

### Frontend

Install dependencies:

```bash
yarn install
```

Build plugin in development mode and run in watch mode:

```bash
npm run dev
```

4. Run the tests (using Jest)

```bash
# Runs the tests and watches for changes, requires git init first
npm run test

# Exits after running all the tests
npm run test:ci
```

Spin up a Grafana instance and run the plugin inside it (using Docker):

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

