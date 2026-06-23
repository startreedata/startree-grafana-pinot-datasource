// Self-test for healthcheck.mjs against a stub Pinot controller+broker.
// Run: `node tests/healthcheck.selftest.mjs`. No framework — asserts + exit code.

import assert from 'node:assert/strict';
import http from 'node:http';
import { runHealthCheck } from './healthcheck.mjs';

// Stub that fakes the Pinot REST surface the health check touches. `rowCount`
// controls what SELECT COUNT(*) returns so we can drive both pass and timeout.
function startStub({ rowCount }) {
  const seen = { schemaCreated: false, tableCreated: false, ingested: false, deletes: [] };
  const server = http.createServer((req, res) => {
    const reply = (obj) => res.end(JSON.stringify(obj));
    if (req.method === 'GET' && req.url === '/tenants') {
      return reply({ BROKER_TENANTS: ['DefaultTenant'], SERVER_TENANTS: ['DefaultTenant'] });
    }
    if (req.method === 'POST' && req.url === '/schemas') {
      seen.schemaCreated = true;
      return reply({ status: 'ok' });
    }
    if (req.method === 'POST' && req.url === '/tables') {
      seen.tableCreated = true;
      return reply({ status: 'ok' });
    }
    if (req.method === 'POST' && req.url.startsWith('/ingestFromFile')) {
      seen.ingested = true;
      return reply({ status: 'ok' });
    }
    if (req.method === 'POST' && req.url === '/query/sql') {
      return reply({ resultTable: { rows: [[rowCount]] }, exceptions: [] });
    }
    if (req.method === 'DELETE') {
      seen.deletes.push(req.url);
      return reply({ status: 'ok' });
    }
    res.statusCode = 404;
    res.end('not found');
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      resolve({ url: `http://127.0.0.1:${port}`, seen, close: () => server.close() });
    });
  });
}

async function main() {
  // Happy path: data shows up, round-trip succeeds, table is cleaned up.
  {
    const stub = await startStub({ rowCount: 3 });
    await runHealthCheck({
      controllerUrl: stub.url,
      brokerUrl: stub.url,
      database: 'default',
      authToken: 'tok',
      timeoutMs: 10000,
      table: 'hc_test',
    });
    assert.ok(stub.seen.schemaCreated, 'schema should be created');
    assert.ok(stub.seen.tableCreated, 'table should be created');
    assert.ok(stub.seen.ingested, 'data should be ingested');
    assert.equal(stub.seen.deletes.length, 2, 'table + schema should be deleted');
    stub.close();
  }

  // Timeout path: data never appears, so it must reject — and still clean up.
  {
    const stub = await startStub({ rowCount: 0 });
    await assert.rejects(
      runHealthCheck({
        controllerUrl: stub.url,
        brokerUrl: stub.url,
        database: 'default',
        authToken: 'tok',
        timeoutMs: 1500,
        table: 'hc_test',
      }),
      /timed out|aborted/
    );
    assert.equal(stub.seen.deletes.length, 2, 'cleanup must run even on timeout');
    stub.close();
  }

  console.log('healthcheck.selftest: OK');
}

main().catch((err) => {
  console.error('healthcheck.selftest: FAILED', err);
  process.exit(1);
});
