/**
 * MEM-01 pilot bounds through the real CLI entrypoint (spawned child, isolated
 * HOME/GBRAIN_HOME, no database URL, no provider keys).
 *
 *   - `--job-ids` / `--max-model-attempts` pass the global pre-dispatch flag
 *     validator (never "unknown flag") and bad values are rejected by the
 *     drain parser with exit 2 before any brain is configured.
 *   - `--max-model-attempts` without an explicit native `--model google:…` is
 *     rejected by the drain parser (supported pilot scope).
 *   - a `--job-ids` preview against a disk PGLite brain reports counts scoped
 *     to the selection and leaves every row byte-identical; a selected `--yes`
 *     touches only the selection; ordinary documents keep the legacy keys.
 *   - Row identity is read only through `MinionQueue.getJob`; SQL is used for
 *     nothing but seeding.
 *
 * Serial: spawns the CLI and opens a disk PGLite brain between spawns.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { MinionQueue } from '../src/core/minions/queue.ts';
import { runCli } from './helpers/cli-spawn.ts';

let emptyHome: string;
let brainHome: string;
let dbPath: string;
let chosen: number;
let seeded: number[] = [];

async function withBrain<T>(fn: (engine: PGLiteEngine) => Promise<T>): Promise<T> {
  const engine = new PGLiteEngine();
  await engine.connect({ database_path: dbPath });
  try { return await fn(engine); } finally { await engine.disconnect(); }
}
/** Every seeded job through the public getter: id, status and a byte-level snapshot. */
async function jobs(engine: PGLiteEngine): Promise<Array<{ id: number; status: string | undefined; r: string }>> {
  const q = new MinionQueue(engine);
  return Promise.all(seeded.map(async id => {
    const job = await q.getJob(id);
    return { id, status: job?.status, r: JSON.stringify(job) };
  }));
}

beforeAll(async () => {
  emptyHome = mkdtempSync(join(tmpdir(), 'gbrain-pilot-cli-empty-'));
  brainHome = mkdtempSync(join(tmpdir(), 'gbrain-pilot-cli-brain-'));
  mkdirSync(join(brainHome, '.gbrain'), { recursive: true });
  dbPath = join(brainHome, '.gbrain', 'brain.pglite');
  writeFileSync(join(brainHome, '.gbrain', 'config.json'),
    JSON.stringify({ engine: 'pglite', database_path: dbPath, embedding_dimensions: 1536 }) + '\n');
  await withBrain(async (engine) => {
    await engine.initSchema();
    await engine.setConfig('facts.extraction_enabled', 'false');
    const q = new MinionQueue(engine);
    const data = { slug: 'meetings/pilot-cli-example', sourceId: 'default', source: 'mcp:put_page', notabilityFilter: 'all', visibility: 'private' };
    seeded = [
      (await q.add('facts-absorb', data, { priority: -10, max_attempts: 5 })).id,
      (await q.add('facts-absorb', data, { max_attempts: 5 })).id,
      (await q.add('facts-absorb', data, { max_attempts: 5 })).id,
    ];
    chosen = seeded[2]!;
  });
}, 180_000);

afterAll(() => {
  rmSync(emptyHome, { recursive: true, force: true });
  rmSync(brainHome, { recursive: true, force: true });
});

describe('engine-free validation', () => {
  test('--help documents both flags', async () => {
    const r = await runCli(['jobs', 'drain', '--help'], { home: emptyHome });
    expect(r.exitCode).toBe(0);
    expect(r.stdout).toContain('--job-ids');
    expect(r.stdout).toContain('--max-model-attempts');
  });

  test.each([
    [['--job-ids', '1,1'], '--job-ids'],
    [['--job-ids', '1,2,3,4,5'], '--job-ids'],
    [['--job-ids=0'], '--job-ids'],
    [['--max-model-attempts', '17'], '--max-model-attempts'],
    [['--max-model-attempts=0'], '--max-model-attempts'],
    [['--max-model-attempts', '2'], '--max-model-attempts requires --model google:'],
    [['--model', 'openai:gpt-5.2', '--max-model-attempts', '2'], '--max-model-attempts requires --model google:'],
  ])('rejects %j in the drain parser, before any engine connection', async (args, needle) => {
    const r = await runCli(['jobs', 'drain', 'facts-absorb', '--max-jobs', '1', '--max-usd', '1', ...(args as string[])], { home: emptyHome });
    expect(r.exitCode).toBe(2);
    expect(r.stderr).toContain(`gbrain jobs drain: ${needle as string}`);
    expect(r.stderr).not.toContain('unknown flag');
    expect(r.stderr).not.toContain('No brain configured');
  });
});

/** The deployed consumer's document keys, in order (independent literal). */
const PREVIEW_KEYS = ['schema_version', 'mode', 'name', 'queue', 'model_override', 'counts', 'oldest_waiting_age_seconds',
  'extraction_enabled', 'chat', 'embedding', 'pricing', 'blockers', 'would_promote_at_most', 'would_attempt_at_most'];
const REPORT_KEYS = ['schema_version', 'mode', 'name', 'queue', 'model_override', 'outcome', 'blockers', 'extraction_enabled',
  'promoted', 'attempted', 'handler_returned', 'handler_threw', 'durable', 'released_unrun', 'domain_stop_confirmed',
  'in_flight_unconfirmed', 'skipped', 'failures', 'facts_inserted', 'remaining', 'budget', 'elapsed_ms', 'max_runtime_ms'];
const GOOGLE = 'google:gemini-3.8-flash';
const lastJson = (stdout: string) => JSON.parse(stdout.trim().split('\n').pop()!);
const drain = (...extra: string[]) => runCli(['jobs', 'drain', 'facts-absorb', '--max-jobs', '5', '--max-usd', '0.5',
  '--max-runtime-min', '2', '--json', ...extra], { home: brainHome, timeoutMs: 150_000 });

describe('against a disk PGLite brain', () => {
  test('an ordinary preview keeps exactly the legacy document', async () => {
    const r = await drain();
    expect(r.exitCode).toBe(0);
    expect(Object.keys(lastJson(r.stdout))).toEqual(PREVIEW_KEYS);
  }, 150_000);

  test('a --job-ids preview is scoped to the selection and read-only', async () => {
    const before = await withBrain(jobs);
    const r = await drain('--job-ids', String(chosen), '--model', GOOGLE, '--max-model-attempts', '2');
    expect(r.exitCode).toBe(0);
    const preview = lastJson(r.stdout);
    expect(Object.keys(preview)).toEqual([...PREVIEW_KEYS, 'job_ids', 'max_model_attempts', 'models']);
    expect(preview.mode).toBe('preview');
    expect(preview.job_ids).toEqual([chosen]);
    expect(preview.max_model_attempts).toBe(2);
    expect(preview.models).toEqual({ chat: null, embedding: null });   // extraction disabled: nothing requested
    expect(preview.counts.waiting).toBe(1);
    expect(await withBrain(jobs)).toEqual(before);
  }, 150_000);

  test('a selected --yes run executes only the selection; an ordinary --yes keeps the legacy report', async () => {
    const before = await withBrain(jobs);
    const selected = await drain('--job-ids', String(chosen), '--model', GOOGLE, '--max-model-attempts', '2', '--yes');
    expect(selected.exitCode).toBe(0);
    const report = lastJson(selected.stdout);
    expect(Object.keys(report)).toEqual([...REPORT_KEYS, 'job_ids', 'model_attempts', 'models']);
    expect(report.outcome).toBe('empty');
    expect(report.attempted).toBe(1);
    expect(report.model_attempts).toEqual({ attempted: 0, max: 2, exhausted: false });
    const after = await withBrain(jobs);
    const others = (list: typeof before) => list.filter(x => x.id !== chosen);
    expect(others(before).map(x => x.status)).toEqual(['waiting', 'waiting']);
    expect(others(after)).toEqual(others(before));
    expect(after.find(x => x.id === chosen)?.status).toBe('completed');

    const ordinary = await drain('--yes');
    expect(ordinary.exitCode).toBe(0);
    expect(Object.keys(lastJson(ordinary.stdout))).toEqual(REPORT_KEYS);
    expect((await withBrain(jobs)).map(x => x.status)).toEqual(['completed', 'completed', 'completed']);
  }, 300_000);
});
