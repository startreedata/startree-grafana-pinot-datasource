# Development

**Frontend:** React, Typescript, Jest, Playwright

**Backend:** Golang, Mage

### Dev Dependencies

| Dependency | Version | Mac Install                                    | Download                                                |
|------------|---------|------------------------------------------------|---------------------------------------------------------|
| Golang     | 1.23.0  | `brew install go@1.23`                         | https://go.dev/dl/                                      |
| Mage       | 1.15.0  | `brew install mage`                            | https://magefile.org/                                   |
| Docker     | -       |                                                | https://docs.docker.com/desktop/install/mac-install/    |
| NodeJs     | 20.20.2 | `brew install nvm && nvm install`              | https://nodejs.org/en/download                          |
| npm        | 10.8.2  | Bundled with Node.js 20.20.2                    | https://docs.npmjs.com/cli/v10/                         |
| Playwright | 1.41.2  | `npm exec -- playwright install --with-deps chromium` | https://playwright.dev/docs/intro                       |

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
nvm use
npm ci
```

Use the committed `package-lock.json` with npm 10.8.2. The checked-in `.npmrc` sets
`legacy-peer-deps=true` to preserve Yarn Classic's existing peer dependency handling
for Grafana 10 and React 17.

#### Braces security override

The build dependencies temporarily replace `braces` with
`@dieub/braces-depth-guard@3.0.3-pn.3` to address
[CVE-2026-93687](https://github.com/advisories/GHSA-vfj7-8cjw-p6xm). The published fork
was checked against [source commit 305a2e4bfe324bb53c336c1b03387ee1251c926f](https://github.com/dieub/braces-depth-guard/commit/305a2e4bfe324bb53c336c1b03387ee1251c926f).
It limits combined brace/parenthesis nesting to 100. Deeper strings throw
`SyntaxError`; direct AST operations reject excessive depth with `RangeError`.
This intentional input limit prevents recursive stack exhaustion.

npm's lockfile records the installed fork's name, version, URL, and integrity.
Yarn Classic's alias lockfile causes Grafana's dependency scanner to identify the
original `braces` package instead. The npm lockfile makes the patched package identity
explicit to scanners; targeted overrides retain runtime versions npm would otherwise
downgrade. When upstream publishes a reviewed fix, remove only the `braces` override,
regenerate the lockfile with npm 10.8.2, and repeat the frontend checks, production
build, and full Grafana validation.

Launch backend in a separate terminal:

```bash
npm run dev:backend
```

Build plugin in development mode and run in watch mode:

```bash
npm run dev
```

#### Unit tests

Unit tests are written in [Jest](https://jestjs.io/).

Run unit tests:

```bash
npm run test
```

#### E2E tests

* E2E tests are written in [Playwright](https://playwright.dev/).
* Test specs are located in `tests/`.
* See [Grafana Plugin Features](https://docs.google.com/spreadsheets/d/1pRcVIEchaQ2S25uydlLbOh0IBYkwQtxgL5eMrm1c0v8/edit?gid=0#gid=0) for E2E test coverage.


Set the following environment variables in `tests/.env`:

```bash
PINOT_CONNECTION_CONTROLLER_URL="https://pinot.celpxu.cp.s7e.startree.cloud"
PINOT_CONNECTION_BROKER_URL="https://broker.pinot.celpxu.cp.s7e.startree.cloud"
PINOT_CONNECTION_DATABASE="ws_2jkxph6tf0nr"
PINOT_CONNECTION_AUTH_TOKEN="st-..."
```

Launch the backend in a separate terminal:

```bash
npm run dev:backend
```

Launch the E2E tests:

```bash
npm run test:e2e
```

Launch E2E UI

```bash
npm run test:e2e:ui
```
