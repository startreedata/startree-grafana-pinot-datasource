// Pinot cluster health check — the e2e CI pre-flight gate.
//
// Does a full round-trip against the remote Pinot cluster (create table, load
// sample data, query it back, delete it) within a deadline. If the cluster
// can't manage that in time, the process exits non-zero so CI skips the
// Playwright suite instead of hanging on a sick cluster.
//
// Uses only Node built-ins (global fetch/FormData/Blob, Node 18+), so the e2e
// job needs no extra toolchain. Run: `node tests/healthcheck.mjs`.
//
// Env: PINOT_CONNECTION_{CONTROLLER_URL,BROKER_URL,DATABASE,AUTH_TOKEN},
//      HEALTHCHECK_TIMEOUT_SECONDS (default 120).

import { fileURLToPath } from 'node:url';

export async function runHealthCheck({ controllerUrl, brokerUrl, database, authToken, timeoutMs, table }) {
  controllerUrl = controllerUrl.replace(/\/$/, '');
  brokerUrl = brokerUrl.replace(/\/$/, '');

  const headers = (extra = {}) => {
    const h = { Accept: 'application/json', ...extra };
    if (authToken) h.Authorization = `Bearer ${authToken}`;
    // Mirror the datasource backend: only send a Database header when it's a
    // non-default named database (see pkg/pinot/client.go).
    if (database && database !== 'default') h.Database = database;
    return h;
  };

  // body: string => JSON; FormData => multipart (fetch sets the boundary); else no body.
  const req = async (method, url, body, signal) => {
    const extra = typeof body === 'string' ? { 'Content-Type': 'application/json' } : {};
    const resp = await fetch(url, { method, headers: headers(extra), body, signal });
    const text = await resp.text();
    if (!resp.ok) {
      throw new Error(`${method} ${url} -> ${resp.status}: ${text.slice(0, 500)}`);
    }
    return text;
  };

  const filePart = (json, filename) => new Blob([json], { type: 'application/json' });

  const discoverTenants = async (signal) => {
    const data = JSON.parse(await req('GET', `${controllerUrl}/tenants`, undefined, signal));
    return {
      broker: (data.BROKER_TENANTS && data.BROKER_TENANTS[0]) || 'DefaultTenant',
      server: (data.SERVER_TENANTS && data.SERVER_TENANTS[0]) || 'DefaultTenant',
    };
  };

  const createSchema = (signal) => {
    const schema = JSON.stringify({
      schemaName: table,
      dimensionFieldSpecs: [{ name: 'id', dataType: 'STRING' }],
      metricFieldSpecs: [{ name: 'value', dataType: 'LONG' }],
      dateTimeFieldSpecs: [
        { name: 'ts', dataType: 'LONG', format: '1:MILLISECONDS:EPOCH', granularity: '1:MILLISECONDS' },
      ],
    });
    const fd = new FormData();
    fd.append('schemaName', filePart(schema), `${table}.json`);
    return req('POST', `${controllerUrl}/schemas`, fd, signal);
  };

  const createTable = ({ broker, server }, signal) => {
    const config = JSON.stringify({
      tableName: table,
      tableType: 'OFFLINE',
      segmentsConfig: { timeColumnName: 'ts', schemaName: table, replication: '1' },
      tenants: { broker, server },
      tableIndexConfig: { loadMode: 'MMAP' },
      metadata: {},
    });
    return req('POST', `${controllerUrl}/tables`, config, signal);
  };

  const ingest = (signal) => {
    const now = Date.now();
    const data = JSON.stringify([
      { id: 'a', value: 1, ts: now },
      { id: 'b', value: 2, ts: now },
      { id: 'c', value: 3, ts: now },
    ]);
    const fd = new FormData();
    fd.append('file', filePart(data), 'data.json');
    const params = new URLSearchParams({
      tableNameWithType: `${table}_OFFLINE`,
      batchConfigMapStr: JSON.stringify({ inputFormat: 'json' }),
    });
    return req('POST', `${controllerUrl}/ingestFromFile?${params}`, fd, signal);
  };

  const count = async (signal) => {
    const data = JSON.parse(
      await req('POST', `${brokerUrl}/query/sql`, JSON.stringify({ sql: `SELECT COUNT(*) FROM ${table}` }), signal)
    );
    if (data.exceptions && data.exceptions.length) {
      throw new Error(`query exception: ${data.exceptions[0].message}`);
    }
    const rows = data.resultTable && data.resultTable.rows;
    return rows && rows.length ? Number(rows[0][0]) : 0;
  };

  const waitForRows = async (want, signal) => {
    for (;;) {
      let n = 0;
      // Ingested data isn't queryable instantly; tolerate transient query errors
      // and keep polling until the data shows up or the deadline aborts us.
      try {
        n = await count(signal);
      } catch (e) {
        if (signal.aborted) throw e;
      }
      if (n >= want) return;
      if (signal.aborted) throw new Error(`timed out waiting for ${want} rows (last count: ${n})`);
      await sleep(2000, signal);
    }
  };

  const cleanup = async () => {
    // Best-effort, on a fresh deadline so it still runs after a timeout.
    const signal = AbortSignal.timeout(30000);
    for (const url of [`${controllerUrl}/tables/${table}?type=OFFLINE`, `${controllerUrl}/schemas/${table}`]) {
      try {
        await req('DELETE', url, undefined, signal);
      } catch (e) {
        // ignore — we don't want cleanup noise to mask the real result
      }
    }
  };

  const signal = AbortSignal.timeout(timeoutMs);
  try {
    const tenants = await discoverTenants(signal);
    console.log(`Health check: table=${table} tenants=${JSON.stringify(tenants)} deadline=${timeoutMs / 1000}s`);
    await createSchema(signal);
    await createTable(tenants, signal);
    await ingest(signal);
    await waitForRows(3, signal);
  } finally {
    await cleanup();
  }
}

function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(resolve, ms);
    signal.addEventListener(
      'abort',
      () => {
        clearTimeout(t);
        reject(new Error('aborted'));
      },
      { once: true }
    );
  });
}

// Run when invoked directly (not when imported by the self-test).
if (process.argv[1] && process.argv[1] === fileURLToPath(import.meta.url)) {
  const controllerUrl = process.env.PINOT_CONNECTION_CONTROLLER_URL || '';
  const brokerUrl = process.env.PINOT_CONNECTION_BROKER_URL || '';
  if (!controllerUrl || !brokerUrl) {
    console.error('PINOT_CONNECTION_CONTROLLER_URL and PINOT_CONNECTION_BROKER_URL must be set');
    process.exit(1);
  }

  const start = Date.now();
  runHealthCheck({
    controllerUrl,
    brokerUrl,
    database: process.env.PINOT_CONNECTION_DATABASE || '',
    authToken: process.env.PINOT_CONNECTION_AUTH_TOKEN || '',
    timeoutMs: (Number(process.env.HEALTHCHECK_TIMEOUT_SECONDS) || 120) * 1000,
    table: `grafana_healthcheck_${Date.now()}`,
  })
    .then(() => {
      console.log(`Pinot health check PASSED in ${((Date.now() - start) / 1000).toFixed(1)}s`);
      process.exit(0);
    })
    .catch((err) => {
      console.error(`Pinot health check FAILED after ${((Date.now() - start) / 1000).toFixed(1)}s: ${err.message || err}`);
      process.exit(1);
    });
}
