/** Launched only by the browser fixture, from a temporary cwd with an isolated home. */
import { PGLiteEngine } from '../../src/core/pglite-engine.ts';
import { runServeHttp } from '../../src/commands/serve-http.ts';

if (!process.env.GBRAIN_TEST_HTTP_PUBLIC_URL || !process.env.GBRAIN_TEST_HTTP_PORT) throw new Error('Browser fixture requires an explicit loopback endpoint');
const engine = new PGLiteEngine();
await engine.connect({});
await engine.initSchema();
// A second source, so the first-consent source picker (#6202) has a real choice.
await engine.executeRaw(`INSERT INTO sources (id, name) VALUES ('wiki-example', 'wiki-example') ON CONFLICT (id) DO NOTHING`);
// Opt-in synthetic queue state for the Jobs Watch journey: four waiting jobs
// created 1234 minutes ago, no active row, no completed row, no worker evidence.
if (process.env.GBRAIN_TEST_BROWSER_SEED === 'jobs-watch-wedged') {
  for (let i = 0; i < 4; i++) {
    await engine.executeRaw(`INSERT INTO minion_jobs (submission_authority, name, queue, status, data, created_at, updated_at)
      VALUES ('{"version":1,"kind":"application"}'::jsonb, 'synthetic-backlog', 'default', 'waiting', '{"note":"synthetic-payload-marker-0b7e"}'::jsonb,
              now() - interval '1234 minutes', now() - interval '1234 minutes')`);
  }
}
// Opt-in synthetic facts-absorb receipts, sources and ordinary bearers for the Fact Review journey.
if (process.env.GBRAIN_TEST_BROWSER_SEED === 'fact-review') await (await import('./fact-review-seed.ts')).seedFactReview(engine, process.env.GBRAIN_TEST_HTTP_PUBLIC_URL);
try {
  await runServeHttp(engine, { port: Number(process.env.GBRAIN_TEST_HTTP_PORT), publicUrl: process.env.GBRAIN_TEST_HTTP_PUBLIC_URL,
    tokenTtl: 3600, enableDcr: true, bind: '127.0.0.1' });
} finally { await engine.disconnect(); }
