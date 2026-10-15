# Releases

The **Create GitHub Release** workflow bumps the selected version, runs the release gates, and creates a draft GitHub release. Branch releases write the complete prerelease version into `package.json` before building, so the tag, plugin metadata, and archive agree. E2E tests may be skipped only with the explicit `skip_e2e` input; the build and frontend/backend tests must still pass.

The **Create Customer Release** workflow re-signs an existing internal Artifactory artifact for the supplied Grafana root URLs. Set its optional `version` input to the exact published internal artifact version, for example `1.1.0`. A leading `v` is accepted. The CLI equivalent is:

```sh
RELEASE_VERSION=1.1.0 RELEASE_NAME=example go run cmd/release/main.go resign https://grafana.example.com
```

The command also requires `ARTIFACTORY_TOKEN` and `GRAFANA_ACCESS_POLICY_TOKEN`. It reads `startree-pinot-datasource-1.1.0.zip` and writes `startree-pinot-datasource-1.1.0-example.zip`. The selected version must be valid SemVer, and the internal artifact must exist before re-signing begins.

When the version input or `RELEASE_VERSION` is empty, customer releases retain the existing latest Git tag selection (`git tag --list --sort=-v:refname`). A tag does not prove that an internal artifact exists. Select the artifact version explicitly when the internal release and latest tag differ.
