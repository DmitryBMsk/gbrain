/**
 * `gbrain jobs drain` through the real CLI entrypoint (spawned child, isolated
 * HOME/GBRAIN_HOME, no database URL, no provider keys, --no-env-file).
 *
 *   - --help, unsupported names and invalid bounds are answered with NO brain
 *     configured: exit 0/2 and never "No brain configured" — proof that the
 *     rejection happens before any engine connection, claim or write.
 *   - preview against a real disk PGLite brain leaves every job row
 *     byte-identical and prints counts only (no slug).
 *   - --yes executes only the allowlisted name; older/higher-priority
 *     unrelated rows stay byte-identical.
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

const LONG_BODY = 'synthetic drain meeting note that is comfortably longer than eighty characters '.repeat(3);
let emptyHome: string;
let brainHome: string;
let dbPath: string;

async function withBrain<T>(fn: (engine: PGLiteEngine) => Promise<T>): Promise<T> {
  const engine = new PGLiteEngine();
  await engine.connect({ database_path: dbPath });
  try { return await fn(engine); } finally { await engine.disconnect(); }
}

async function rows(engine: PGLiteEngine): Promise<Array<{ id: number; name: string; queue: string; status: string; r: string }>> {
  return engine.executeRaw(`SELECT id, name, queue, status, row_to_json(j)::text AS r FROM minion_jobs j ORDER BY id`);
}

beforeAll(async () => {
  emptyHome = mkdtempSync(join(tmpdir(), 'gbrain-drain-cli-empty-'));
  brainHome = mkdtempSync(join(tmpdir(), 'gbrain-drain-cli-brain-'));
  mkdirSync(join(brainHome, '.gbrain'), { recursive: true });
  dbPath = join(brainHome, '.gbrain', 'brain.pglite');
  writeFileSync(join(brainHome, '.gbrain', 'config.json'),
    JSON.stringify({ engine: 'pglite', database_path: dbPath, embedding_dimensions: 1536 }) + '\n');
  await withBrain(async (engine) => {
    await engine.initSchema();
    await engine.setConfig('facts.extraction_enabled', 'false');
    const q = new MinionQueue(engine);
    const unrelated = [
      await q.add('sync', {}, { priority: -10 }),
      await q.add('embed', {}, { priority: -5 }),
      await q.add('extract', {}, {}),
    ];
    await engine.executeRaw(`UPDATE minion_jobs SET created_at = now() - interval '10 hours' WHERE id = ANY($1::int[])`, [unrelated.map(j => j.id)]);
    await engine.executeRaw(`UPDATE minion_jobs SET status = 'delayed', delay_until = now() - interval '1 minute' WHERE id = $1`, [unrelated[2].id]);
    for (const n of [1, 2]) {
      const slug = `meetings/drain-cli-example-${n}`;
      await engine.putPage(slug, { type: 'meeting', title: `Drain CLI example ${n}`, compiled_truth: LONG_BODY }, { sourceId: 'default' } as never);
      await q.add('facts-absorb', { slug, sourceId: 'default', source: 'mcp:put_page', notabilityFilter: 'all', visibility: 'private' }, { max_attempts: 5 });
    }
  });
}, 180_000);

afterAll(() => {
  rmSync(emptyHome, { recursive: true, force: true });
  rmSync(brainHome, { recursive: true, force: true });
});

describe('engine-free validation', () => {
  test('--help prints drain help without a brain', async () => {
    const r = await runCli(['jobs', 'drain', '--help'], { home: emptyHome });
    expect(r.exitCode).toBe(0);
    expect(r.stdout).toContain('gbrain jobs drain');
    expect(r.stdout).toContain('--max-jobs');
    expect(r.stdout).toContain('--max-runtime-min');
  });

  test.each([
    [['shell', '--max-jobs', '1', '--max-usd', '1'], 'not drainable'],
    [['sync', '--max-jobs', '1', '--max-usd', '1', '--yes'], 'not drainable'],
    [['facts-absorb', '--max-jobs', '0', '--max-usd', '1'], '--max-jobs'],
    [['facts-absorb', '--max-jobs', '1'], '--max-usd'],
    [['facts-absorb', '--max-jobs', '1', '--max-usd', '1', '--queue', 'other-queue'], '--queue'],
    [['facts-absorb', '--max-jobs', '1', '--max-usd', '1', '--concurrency', '4'], '--concurrency'],
  ])('rejects %j before any engine connection', async (args, needle) => {
    const r = await runCli(['jobs', 'drain', ...(args as string[])], { home: emptyHome });
    expect(r.exitCode).toBe(2);
    expect(r.stderr).toContain(needle as string);
    expect(r.stderr).not.toContain('No brain configured');
  });
});

describe('against a disk PGLite brain', () => {
  test('preview is read-only and prints counts only', async () => {
    const before = await withBrain(rows);
    const r = await runCli(['jobs', 'drain', 'facts-absorb', '--max-jobs', '1', '--max-usd', '0.5', '--json'], { home: brainHome, timeoutMs: 120_000 });
    expect(r.exitCode).toBe(0);
    const preview = JSON.parse(r.stdout.trim().split('\n').pop()!);
    expect(preview.mode).toBe('preview');
    expect(preview.counts).toEqual({ waiting: 2, delayed: 0, delayed_due: 0, delayed_future: 0, active: 0, active_expired: 0 });
    expect(preview.extraction_enabled).toBe(false);
    expect(r.stdout + r.stderr).not.toContain('drain-cli-example');
    expect(await withBrain(rows)).toEqual(before);
  }, 150_000);

  test('--yes drains only facts-absorb; unrelated older/higher-priority rows are byte-identical', async () => {
    const before = await withBrain(rows);
    const r = await runCli(['jobs', 'drain', 'facts-absorb', '--max-jobs', '5', '--max-usd', '0.5', '--max-runtime-min', '2', '--yes', '--json'],
      { home: brainHome, timeoutMs: 150_000 });
    expect(r.exitCode).toBe(0);
    const report = JSON.parse(r.stdout.trim().split('\n').pop()!);
    expect(report.mode).toBe('execute');
    expect(report.outcome).toBe('empty');
    expect(report.attempted).toBe(2);
    expect(report.skipped).toEqual({ extraction_disabled: 2 });
    expect(r.stdout + r.stderr).not.toContain('drain-cli-example');
    const after = await withBrain(rows);
    const unrelatedBefore = before.filter(x => x.name !== 'facts-absorb').map(x => x.r);
    expect(after.filter(x => x.name !== 'facts-absorb').map(x => x.r)).toEqual(unrelatedBefore);
    expect(after.filter(x => x.name === 'facts-absorb').map(x => x.status)).toEqual(['completed', 'completed']);
  }, 180_000);
});
