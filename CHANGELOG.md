# Changelog

## 1.1.9 (2026-11-06)

- Send query options in the request body's `queryOptions` field instead of as `SET` statements in the SQL, so they work when `sqlOptionsMode` is `IGNORE` or `REJECT`.
- Add a Query Options editor to code mode.
- The SQL preview shows the SQL sent to Pinot, without the query options.
- Stop logging query text and request URLs at Info level; query text is now logged at Debug level only, and requests log only method, path and status.
- Keep upstream response bodies for non-200 Pinot responses out of logs; they are still shown in the panel error.
- Return an error instead of panicking when a time-series response has inconsistent labels.

## 1.1.8 (2026-10-29)

- Bounds-check INT column values before converting them to 32-bit integers (gosec G115).

## 1.1.7 (2026-10-29)

- Bump `klauspost/compress` to v1.20.3 to address CVE-2026-116591.

## 1.1.6 (2026-10-29)

- Support `FLOAT_ARRAY` columns in query results.
- Resolve Snyk-reported vulnerabilities in Go dependencies and the Cloud Docker image.

## 1.1.5 (2026-10-25)

- Build the backend with Go 1.26.10.
- Make release recovery safer and pin signing tools.

## 1.1.4 (2026-10-24)

- Patch a critical `dompurify` advisory via an npm override.
- Patch dev-dependency advisories for `fast-uri` and `serialize-javascript`.
- Pin CI E2E runners to Ubuntu 24.04.

## 1.1.3 (2026-10-17)

- Upgrade Grafana Go SDK to v0.297.0 and its required backend dependencies, clearing Grafana's SDK-age warning.

## 1.1.2 (2026-10-17)

- Update Go and backend dependencies to address Grafana security findings.
- Keep Go type constraints compatible with Grafana's source scanner and stop release builds when backend compilation fails.
- Replace vulnerable braces with a pinned security fork, update serialize-javascript, and use npm for reproducible dependency identity and builds.

## 1.0.0 (Unreleased)

Initial release.
