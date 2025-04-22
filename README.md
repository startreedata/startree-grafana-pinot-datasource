# Pinot Data Source plugin for Grafana

The Pinot Data Source Plugin for Grafana allows you to create visualizations in Grafana from data in your Pinot cluster.

**System Requirements**

Grafana version >= 9.1.1

## Development

**Frontend:** React, Typescript, Jest, Playwright

**Backend:** Golang, Mage

### Dev Dependencies

| Dependency | Version | Mac Install                                    | Download                                                |
|------------|---------|------------------------------------------------|---------------------------------------------------------|
| Golang     | 1.23.0  | `brew install go@1.23`                         | https://go.dev/dl/                                      |
| Mage       | 1.15.0  | `brew install mage`                            | https://magefile.org/                                   |
| Docker     | -       |                                                | https://docs.docker.com/desktop/install/mac-install/    |
| NodeJs     | 20      | `brew install nvm && nvm install 20`           | https://nodejs.org/en/download                          |
| Yarn       | 1.22.19 | `npm install -g yarn`                          | https://classic.yarnpkg.com/en/docs/install/#mac-stable |
| Playwright | 1.41.2  | `yarn playwright install --with-deps chromium` | https://playwright.dev/docs/intro                       |

### Backend

The backend code handles all interactions with Pinot and provides the data to the frontend.

Relevant directories:

| Description                          | Location               |
|--------------------------------------|------------------------|
| Pinot client code                    | `pkg/pinot/`           |
| Plugin specific code                 | `pkg/plugin/`          |
| Query handlers for visualizations    | `pkg/plugin/dataquery` |
| Resource handlers for UI components. | `pkg/plugin/resources` |
| Helper tools                         | `cmd/`                 |

Build backend plugin binaries:

```bash
mage -v
```

Run backend tests:

```bash
docker compose up pinot --detach --wait --wait-timeout 500
go run cmd/testsetup/main.go
go test ./... -v
```

### Frontend

The frontend code handles all interactions with Grafana and provides the UI components.

Relevant directories:

| Description                | Location                                | Notes                                                                                                                     |
|----------------------------|-----------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| Plugin entrypoint          | `src/module.ts`                         |                                                                                                                           |
| Data source config editor  | `src/components/ConfigEditor`           | [demo](https://drive.google.com/file/d/1DR87qj90xMRnpaXbLffAD2VfoyPK8SpV/view?usp=drive_link)                             |
| Panel/explore query editor | `src/components/QueryEditor`            | [demo](https://drive.google.com/file/d/1DR87qj90xMRnpaXbLffAD2VfoyPK8SpV/view?usp=drive_link)                             |
| Annotations query editor   | `src/components/AnnotationsQueryEditor` | [demo](https://startreedata.slack.com/archives/C071PS6ND1B/p1738709570181519)                                             |
| Variable query editor      | `src/components/VariableQueryEditor`    | [demo](https://startreedata.slack.com/archives/C071PS6ND1B/p1725653129358349?thread_ts=1725653095.452419&cid=C071PS6ND1B) |
| Resource fetchers          | `src/resources`                         |                                                                                                                           |
| E2E Tests                  | `tests/`                                |                                                                                                                           |

Install dependencies:

```bash
yarn install
```

Build plugin in development mode and run in watch mode:

```bash
yarn run dev
```

#### Unit tests

Unit tests are written in [Jest](https://jestjs.io/).

```bash
yarn run test
```

# E2E tests

E2E tests are written in [Playwright](https://playwright.dev/).
Test specs are located in `tests/`.

Set the following environment variables in `tests/.env`:

```bash
PINOT_CONNECTION_CONTROLLER_URL="https://pinot.celpxu.cp.s7e.startree.cloud"
PINOT_CONNECTION_BROKER_URL="https://broker.pinot.celpxu.cp.s7e.startree.cloud"
PINOT_CONNECTION_DATABASE="ws_2jkxph6tf0nr"
PINOT_CONNECTION_AUTH_TOKEN="st-..."
```

Launch the backend in a separate terminal:

```bash
yarn run dev:backend
```

Launch the E2E tests:

```bash
yarn run test:e2e
```

Launch E2E UI

```bash
yarn run test:e2e:ui
```

## Release

New releases should be tested and approved by [#ask-galileo](https://startreedata.slack.com/archives/C06LUQ8UYD6).

Pushing a new version tag should automatically trigger a new unsigned release build.

Due to plugin signing restrictions, we have to create a release artifact for each intended grafana instance. These
_installable_ releases are signed copies of the unsigned release and only valid for the intended grafana instance.

[Create new signed releases.](https://github.com/startreedata/startree-grafana-pinot-datasource/actions/workflows/customer-release.yml)

### Push a version tag

1. Run `npm version <major|minor|patch>`
2. Run `git push origin main --follow-tags`

