# Pinot Data Source plugin for Grafana

## Getting started

### Backend

1. Update [Grafana plugin SDK for Go](https://grafana.com/docs/grafana/latest/developers/plugins/backend/grafana-plugin-sdk-for-go/) dependency to the latest minor version:

```bash
go get -u github.com/grafana/grafana-plugin-sdk-go
go mod tidy
```

2. Build backend plugin binaries for Linux, Windows and Darwin:

```bash
mage -v
```

3. List all available Mage targets for additional commands:

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


#### Push a version tag

To trigger the workflow we need to push a version tag to github. This can be achieved with the following steps:

1. Run `npm version <major|minor|patch>`
2. Run `git push origin main --follow-tags`
