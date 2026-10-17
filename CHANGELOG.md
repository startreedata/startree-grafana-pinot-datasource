# Changelog

## 1.1.3 (2026-10-17)

- Upgrade Grafana Go SDK to v0.297.0 and its required backend dependencies, clearing Grafana's SDK-age warning.

## 1.1.2 (2026-10-17)

- Update Go and backend dependencies to address Grafana security findings.
- Keep Go type constraints compatible with Grafana's source scanner and stop release builds when backend compilation fails.
- Replace vulnerable braces with a pinned security fork, update serialize-javascript, and use npm for reproducible dependency identity and builds.

## 1.0.0 (Unreleased)

Initial release.
