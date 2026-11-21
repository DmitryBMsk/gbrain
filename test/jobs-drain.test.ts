/**
 * `gbrain jobs drain` — bounded, single-handler drain of one queued job type.
 *
 * Shared backend suite: PGLite always; PostgreSQL through
 * test/e2e/jobs-drain-parity.test.ts (registerPostgresTests). Every model
 * call goes through the gateway test transports — no provider, no keys.
 * Fixtures are synthetic (`meetings/drain-example-*`).
 *
 * Contracts pinned here:
 *   - only the allowlisted name on the shared default queue is ever claimed,
 *     even when unrelated rows are older / higher priority;
 *   - no unrelated maintenance: delayed promotion, stall/timeout/TTL sweeps
 *     never touch other rows (worker `queueMaintenance: false`);
 *   - preview is read-only (byte-identical job rows);
 *   - --max-jobs bounds handler executions; a wall-clock bound ends the run
 *     even when nothing can be claimed; in-flight claims past the grace are
 *     released without burning an attempt and a late completion is fenced;
 *   - cost: priced reservations happen inside the drain's BudgetTracker
 *     (ALS reaches the worker's handler), an unpriced model fails closed
 *     before any transport call, a cap below one call spends nothing;
 *   - legacy unreviewed jobs block before any claim and stay unauthorised.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync, existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import type { BrainEngine } from '../src/core/engine.ts';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { MinionQueue } from '../src/core/minions/queue.ts';
import { MinionWorker } from '../src/core/minions/worker.ts';
import type { MinionHandler } from '../src/core/minions/types.ts';
import { RateLeaseUnavailableError } from '../src/core/minions/rate-leases.ts';
import {
  chat,
  __setChatTransportForTests,
  __setEmbedTransportForTests,
  configureGateway,
  resetGateway,
  type ChatResult,
} from '../src/core/ai/gateway.ts';
import { autopilotPausedMarkerPath } from '../src/core/autopilot-paths.ts';
import {
  DRAINABLE_JOB_NAMES,
  DrainUsageError,
  parseDrainArgs,
  previewDrain,
  runDrain,
  type DrainDeps,
  type DrainOptions,
} from '../src/commands/jobs-drain.ts';
import { withEnv } from './helpers/with-env.ts';
import { isolatedPersistencePostgres } from './helpers/persistence-postgres.ts';
import { testBackends } from './helpers/test-backends.ts';

const backends = testBackends();
const engines: BrainEngine[] = [];
let closePostgres: (() => Promise<void>) | undefined;

beforeAll(async () => {
  if (backends.includes('pglite')) {
    const lite = new PGLiteEngine();
    await lite.connect({});
    await lite.initSchema();
    engines.push(lite);
  }
  if (backends.includes('postgres')) {
    const pg = await isolatedPersistencePostgres(process.env.DATABASE_URL!);
    engines.push(pg.engine);
    closePostgres = pg.close;
  }
}, 120_000);

afterAll(async () => {
  for (const engine of engines) if (engine.kind === 'pglite') await engine.disconnect();
  await closePostgres?.();
}, 30_000);

afterEach(() => {
  __setChatTransportForTests(null);
  __setEmbedTransportForTests(null);
  resetGateway();
});

const LONG_BODY = 'synthetic drain meeting note that is comfortably longer than eighty characters '.repeat(3);
const FAST: DrainDeps = { pollMs: 50, workerPollMs: 25, graceMs: 400 };

async function fresh(engine: BrainEngine): Promise<void> {
  await engine.executeRaw('TRUNCATE minion_jobs, facts, pages CASCADE');
  await engine.executeRaw(`DELETE FROM config WHERE key LIKE 'facts.%' OR key LIKE 'models.%' OR key = 'pricing.overrides' OR key LIKE 'drain.test.%'`);
  // Usable synthetic chat + embedding transports by default; keyless / no-embedding
  // cases remove them explicitly. No credential is ever real and no network is used.
  chatStub();
  __setEmbedTransportForTests((async ({ values }: { values: string[] }) =>
    ({ embeddings: values.map(() => Array.from({ length: 1536 }, () => 0.1)) })) as never);
}

async function seedPage(engine: BrainEngine, n: number): Promise<string> {
  const slug = `meetings/drain-example-${n}`;
  await engine.putPage(slug, { type: 'meeting', title: `Drain example ${n}`, compiled_truth: LONG_BODY }, { sourceId: 'default' } as never);
  return slug;
}

interface SeedOpts { queue?: string; priority?: number; ageMin?: number; status?: 'waiting' | 'delayed' | 'active-stalled' | 'cancelled'; data?: Record<string, unknown> }
async function seedJob(engine: BrainEngine, name: string, opts: SeedOpts = {}): Promise<number> {
  const q = new MinionQueue(engine);
  const job = await q.add(name, opts.data ?? {}, { queue: opts.queue ?? 'default', priority: opts.priority ?? 0, max_attempts: 5 });
  const age = opts.ageMin ?? 0;
  if (age) await engine.executeRaw(`UPDATE minion_jobs SET created_at = now() - ($2::int * interval '1 minute') WHERE id = $1`, [job.id, age]);
  if (opts.status === 'delayed') {
    await engine.executeRaw(`UPDATE minion_jobs SET status = 'delayed', delay_until = now() - interval '1 minute' WHERE id = $1`, [job.id]);
  } else if (opts.status === 'active-stalled') {
    await engine.executeRaw(
      `UPDATE minion_jobs SET status = 'active', lock_token = 'foreign-worker:1', lock_until = now() - interval '10 minutes',
         started_at = now() - interval '20 minutes', claim_generation = claim_generation + 1, attempts_started = attempts_started + 1
       WHERE id = $1`, [job.id]);
  } else if (opts.status === 'cancelled') {
    await engine.executeRaw(`UPDATE minion_jobs SET status = 'cancelled' WHERE id = $1`, [job.id]);
  }
  return job.id;
}

async function factsJob(engine: BrainEngine, n: number, extra: SeedOpts = {}): Promise<number> {
  const slug = await seedPage(engine, n);
  return seedJob(engine, 'facts-absorb', {
    ...extra, data: { slug, sourceId: 'default', source: 'mcp:put_page', notabilityFilter: 'all', visibility: 'private', ...(extra.data ?? {}) },
  });
}

/** Unrelated rows that a generic worker would claim or sweep. Older + higher priority on purpose. */
async function seedUnrelated(engine: BrainEngine): Promise<number[]> {
  return [
    await seedJob(engine, 'sync', { priority: -10, ageMin: 600 }),
    await seedJob(engine, 'embed', { priority: -5, ageMin: 500 }),
    await seedJob(engine, 'autopilot-cycle', { ageMin: 400 }),
    await seedJob(engine, 'extract', { status: 'delayed', ageMin: 300 }),
    await seedJob(engine, 'backlinks', { status: 'active-stalled', ageMin: 200 }),
    await seedJob(engine, 'facts-absorb', { queue: 'other-queue', ageMin: 100, data: { slug: 'meetings/elsewhere' } }),
  ];
}

async function rowsById(engine: BrainEngine, ids: number[]): Promise<string[]> {
  const rows = await engine.executeRaw<{ r: string }>(
    `SELECT row_to_json(j)::text AS r FROM minion_jobs j WHERE id = ANY($1::int[]) ORDER BY id`, [ids]);
  return rows.map(row => row.r);
}
async function allRows(engine: BrainEngine): Promise<string[]> {
  const rows = await engine.executeRaw<{ r: string }>(`SELECT row_to_json(j)::text AS r FROM minion_jobs j ORDER BY id`);
  return rows.map(row => row.r);
}
async function job(engine: BrainEngine, id: number) {
  const [row] = await engine.executeRaw<{ status: string; attempts_made: number; attempts_started: number; lock_token: string | null; submission_authority: unknown }>(
    'SELECT status, attempts_made, attempts_started, lock_token, submission_authority FROM minion_jobs WHERE id = $1', [id]);
  return row;
}

function opts(over: Partial<Omit<DrainOptions, 'execute'>> = {}): DrainOptions & { execute: true } {
  return { name: 'facts-absorb', queue: 'default', maxJobs: 10, maxUsd: 1, maxRuntimeMs: 20_000, json: true, ...over, execute: true };
}
function previewOpts(over: Partial<Omit<DrainOptions, 'execute'>> = {}): DrainOptions {
  return { ...opts(over), execute: false };
}

let chatCalls = 0;
function chatStub(usage = { input: 1000, output: 200 }) {
  chatCalls = 0;
  __setChatTransportForTests(async (o): Promise<ChatResult> => {
    chatCalls++;
    return {
      text: JSON.stringify({ facts: [{ fact: `synthetic drain fact ${chatCalls}`, kind: 'event', entity: null, confidence: 1, notability: 'high' }] }),
      blocks: [], stopReason: 'end',
      usage: { input_tokens: usage.input, output_tokens: usage.output, cache_read_tokens: 0, cache_creation_tokens: 0 },
      model: (o as { model?: string }).model ?? 'test:stub', providerId: 'test',
    };
  });
}

/** Register a controllable stand-in under the drained name (lifecycle tests). */
function stubHandlers(handler: MinionHandler): DrainDeps['registerHandlers'] {
  return async (worker) => { worker.register('facts-absorb', handler); worker.register('sync', async () => ({ wrong: true })); };
}

test('anti-vacuity: every selected backend has a live engine', async () => {
  expect(backends.length).toBeGreaterThan(0);
  expect(engines.map(e => e.kind)).toEqual(backends);
  for (const engine of engines) expect((await engine.executeRaw<{ n: number }>('SELECT 1::int AS n'))[0].n).toBe(1);
});

describe('argument contract (engine-free)', () => {
  const base = ['drain', 'facts-absorb', '--max-jobs', '2', '--max-usd', '0.5'];
  test('valid arguments parse with a finite default wall-clock bound, preview by default', () => {
    const parsed = parseDrainArgs(base);
    expect(parsed).toEqual({ name: 'facts-absorb', queue: 'default', maxJobs: 2, maxUsd: 0.5, maxRuntimeMs: 600_000, execute: false, json: false });
    expect(parseDrainArgs([...base, '--yes', '--json', '--max-runtime-min', '2']).maxRuntimeMs).toBe(120_000);
    expect(parseDrainArgs([...base, '--queue', 'default', '--yes']).execute).toBe(true);
    expect([...DRAINABLE_JOB_NAMES]).toEqual(['facts-absorb']);
  });
  test.each([
    [['drain', 'shell', '--max-jobs', '1', '--max-usd', '1']],
    [['drain', 'subagent', '--max-jobs', '1', '--max-usd', '1']],
    [['drain', 'sync', '--max-jobs', '1', '--max-usd', '1']],
    [['drain', 'autopilot-cycle', '--max-jobs', '1', '--max-usd', '1']],
    [['drain', ' facts-absorb', '--max-jobs', '1', '--max-usd', '1']],
    [['drain', '--max-jobs', '1', '--max-usd', '1']],
    [['drain', 'facts-absorb', 'extra', '--max-jobs', '1', '--max-usd', '1']],
    [['drain', 'facts-absorb', '--max-usd', '1']],
    [['drain', 'facts-absorb', '--max-jobs', '1']],
    [['drain', 'facts-absorb', '--max-jobs', '0', '--max-usd', '1']],
    [['drain', 'facts-absorb', '--max-jobs', '-1', '--max-usd', '1']],
    [['drain', 'facts-absorb', '--max-jobs', '1.5', '--max-usd', '1']],
    [['drain', 'facts-absorb', '--max-jobs', '51', '--max-usd', '1']],
    [['drain', 'facts-absorb', '--max-jobs', 'Infinity', '--max-usd', '1']],
    [['drain', 'facts-absorb', '--max-jobs', '1', '--max-usd', '0']],
    [['drain', 'facts-absorb', '--max-jobs', '1', '--max-usd', 'NaN']],
    [['drain', 'facts-absorb', '--max-jobs', '1', '--max-usd', 'Infinity']],
    [['drain', 'facts-absorb', '--max-jobs', '1', '--max-usd', '-3']],
    [['drain', 'facts-absorb', '--max-jobs', '1', '--max-usd', '1', '--max-runtime-min', '0']],
    [['drain', 'facts-absorb', '--max-jobs', '1', '--max-usd', '1', '--max-runtime-min', '61']],
    [['drain', 'facts-absorb', '--max-jobs', '1', '--max-usd', '1', '--queue', 'other-queue']],
    [['drain', 'facts-absorb', '--max-jobs', '1', '--max-usd', '1', '--concurrency', '4']],
    [['drain', 'facts-absorb', '--max-jobs', '1', '--max-usd', '1', '--params', '{}']],
    [['drain', 'facts-absorb', '--max-jobs', '1', '--max-jobs', '2', '--max-usd', '1']],
  ])('rejects %j', (args) => {
    expect(() => parseDrainArgs(args as string[])).toThrow(DrainUsageError);
  });
});

describe('preview is read-only', () => {
  test('counts only the drained name on the default queue and leaves every row byte-identical', async () => {
    for (const engine of engines) {
      await fresh(engine);
      await seedUnrelated(engine);
      await factsJob(engine, 1, { ageMin: 30 });
      await factsJob(engine, 2);
      await factsJob(engine, 3, { status: 'delayed' });
      await factsJob(engine, 4, { status: 'cancelled' });
      const before = await allRows(engine);
      const preview = await previewDrain(engine, previewOpts({ maxJobs: 1 }));
      expect(await allRows(engine)).toEqual(before);
      expect(preview.mode).toBe('preview');
      expect(preview.counts).toEqual({ waiting: 2, delayed: 1, delayed_due: 1, delayed_future: 0, active: 0, active_expired: 0 });
      expect(preview.oldest_waiting_age_seconds).toBeGreaterThanOrEqual(29 * 60);
      expect(preview.would_attempt_at_most).toBe(1);
      expect(preview.blockers).toEqual([]);
      expect(JSON.stringify(preview)).not.toContain('drain-example');
    }
  });

  test('runDrain without execute is the same preview and claims nothing', async () => {
    for (const engine of engines) {
      await fresh(engine);
      await factsJob(engine, 1);
      const before = await allRows(engine);
      const report = await runDrain(engine, previewOpts(), FAST);
      expect(report.mode).toBe('preview');
      expect(await allRows(engine)).toEqual(before);
    }
  });
});

describe('execution isolation and bounds', () => {
  test('only allowlisted jobs run; older/higher-priority unrelated rows and other queues are untouched', async () => {
    for (const engine of engines) {
      await fresh(engine);
      await engine.setConfig('facts.extraction_enabled', 'false');
      const unrelated = await seedUnrelated(engine);
      const before = await rowsById(engine, unrelated);
      const a = await factsJob(engine, 1);
      const b = await factsJob(engine, 2);
      const report = await runDrain(engine, opts(), FAST);
      expect(report.outcome).toBe('empty');
      expect(report.attempted).toBe(2);
      expect(report.durable.completed).toBe(2);
      expect(report.handler_returned).toBe(2);
      expect(report.skipped).toEqual({ extraction_disabled: 2 });
      expect((await job(engine, a)).status).toBe('completed');
      expect((await job(engine, b)).status).toBe('completed');
      expect(await rowsById(engine, unrelated)).toEqual(before);
      expect(JSON.stringify(report)).not.toContain('drain-example');
    }
  });

  test('--max-jobs bounds handler executions; the remainder is never claimed', async () => {
    for (const engine of engines) {
      await fresh(engine);
      await engine.setConfig('facts.extraction_enabled', 'false');
      const ids = [await factsJob(engine, 1), await factsJob(engine, 2), await factsJob(engine, 3)];
      const t0 = Date.now();
      const report = await runDrain(engine, opts({ maxJobs: 2 }), FAST);
      expect(Date.now() - t0).toBeLessThan(10_000); // stop() inside the handler must not deadlock
      expect(report.outcome).toBe('max_jobs');
      expect(report.attempted).toBe(2);
      const states = await Promise.all(ids.map(id => job(engine, id)));
      expect(states.filter(s => s.status === 'completed').length).toBe(2);
      const untouched = states.find(s => s.status === 'waiting')!;
      expect(untouched.attempts_started).toBe(0);
      expect(report.remaining).toEqual({ waiting: 1, delayed: 0, delayed_due: 0, delayed_future: 0, active: 0, active_expired: 0 });
    }
  });

  test('cancelled jobs are never claimed; missing/superseded source pages skip without a model call', async () => {
    for (const engine of engines) {
      await fresh(engine);
      chatStub();
      const cancelled = await factsJob(engine, 1, { status: 'cancelled' });
      await seedJob(engine, 'facts-absorb', { data: { slug: 'meetings/drain-example-missing', sourceId: 'default' } });
      await seedJob(engine, 'facts-absorb', { data: { slug: 'meetings/drain-example-2', sourceId: 'retired-source-example' } });
      const report = await runDrain(engine, opts(), FAST);
      expect(chatCalls).toBe(0);
      expect(report.skipped).toEqual({ page_missing: 2 });
      expect((await job(engine, cancelled)).status).toBe('cancelled');
    }
  });

  test('future-delayed-only queue ends promptly without promoting the delayed row', async () => {
    for (const engine of engines) {
      await fresh(engine);
      const delayed = await factsJob(engine, 1, { status: 'delayed' });
      await engine.executeRaw(`UPDATE minion_jobs SET delay_until = now() + interval '10 minutes' WHERE id = $1`, [delayed]);
      const before = await rowsById(engine, [delayed]);
      const t0 = Date.now();
      const report = await runDrain(engine, opts(), FAST);
      expect(Date.now() - t0).toBeLessThan(5_000);
      expect(report.outcome).toBe('delayed_only');
      expect(report.attempted).toBe(0);
      expect(await rowsById(engine, [delayed])).toEqual(before);
    }
  });

  test('legacy unreviewed jobs block before any claim and remain unauthorised', async () => {
    for (const engine of engines) {
      await fresh(engine);
      await engine.setConfig('facts.extraction_enabled', 'false');
      const legacy = await seedJob(engine, 'sync');
      await engine.executeRaw('UPDATE minion_jobs SET submission_authority = NULL WHERE id = $1', [legacy]);
      await factsJob(engine, 1);
      const before = await allRows(engine);
      const preview = await previewDrain(engine, previewOpts());
      expect(preview.blockers).toContain('unreviewed_jobs');
      const report = await runDrain(engine, opts(), FAST);
      expect(report.outcome).toBe('blocked');
      expect(report.blockers).toContain('unreviewed_jobs');
      expect(report.attempted).toBe(0);
      expect(await allRows(engine)).toEqual(before);
      expect((await job(engine, legacy)).submission_authority).toBeNull();
    }
  });

  test('pause marker blocks before claim', async () => {
    const home = mkdtempSync(join(tmpdir(), 'gbrain-drain-pause-'));
    try {
      await withEnv({ GBRAIN_HOME: home }, async () => {
        const marker = autopilotPausedMarkerPath();
        mkdirSync(dirname(marker), { recursive: true });
        writeFileSync(marker, 'paused by test');
        for (const engine of engines) {
          await fresh(engine);
          await factsJob(engine, 1);
          const before = await allRows(engine);
          const report = await runDrain(engine, opts(), FAST);
          expect(report.outcome).toBe('blocked');
          expect(report.blockers).toContain('paused');
          expect(await allRows(engine)).toEqual(before);
        }
      });
    } finally { rmSync(home, { recursive: true, force: true }); }
  });
});

describe('lifecycle: wall clock, concurrency, halts', () => {
  test('a concurrent regular worker is fenced by claim leases: every job runs exactly once', async () => {
    for (const engine of engines) {
      await fresh(engine);
      const ids: number[] = [];
      for (let i = 1; i <= 6; i++) ids.push(await factsJob(engine, i));
      const runs = new Map<number, number>();
      const slow: MinionHandler = async (j) => { runs.set(j.id, (runs.get(j.id) ?? 0) + 1); await new Promise(r => setTimeout(r, 60)); return { ok: true }; };
      const peer = new MinionWorker(engine, { queue: 'default', pollInterval: 20, healthCheckInterval: 0, queueMaintenance: false });
      peer.register('facts-absorb', slow);
      const peerRun = peer.start();
      const report = await runDrain(engine, opts({ maxJobs: 50 }), { ...FAST, registerHandlers: stubHandlers(slow) });
      await new Promise(r => setTimeout(r, 300));
      peer.stop();
      await peerRun;
      expect([...runs.values()].every(n => n === 1)).toBe(true);
      expect(runs.size).toBe(6);
      expect(report.attempted).toBeLessThanOrEqual(6);
      expect(['empty', 'delayed_only']).toContain(report.outcome);
    }
  });

  test('queue emptied by someone else before any claim: independent observation ends the run', async () => {
    for (const engine of engines) {
      await fresh(engine);
      const id = await factsJob(engine, 1);
      await engine.executeRaw(`UPDATE minion_jobs SET status = 'cancelled' WHERE id = $1`, [id]);
      const report = await runDrain(engine, opts(), FAST);
      expect(report.outcome).toBe('empty');
      expect(report.attempted).toBe(0);
    }
  });

  test('wall-clock bound: a hung handler is released without burning an attempt and its late completion is fenced', async () => {
    for (const engine of engines) {
      await fresh(engine);
      const id = await factsJob(engine, 1);
      let finish: () => void = () => {};
      const hung: MinionHandler = () => new Promise(resolve => { finish = () => resolve({ late: true }); });
      let startP: Promise<void> | undefined;
      const t0 = Date.now();
      const report = await runDrain(engine, opts({ maxRuntimeMs: 500 }), {
        ...FAST, graceMs: 200, registerHandlers: stubHandlers(hung), onStarted: p => { startP = p; },
      });
      const elapsed = Date.now() - t0;
      expect(elapsed).toBeLessThan(500 + 200 + 3_000);
      expect(report.outcome).toBe('time_limit');
      expect(report.attempted).toBe(1);
      expect(report.in_flight_unconfirmed).toBe(1);
      expect(report.domain_stop_confirmed).toBe(false);
      let row = await job(engine, id);
      expect(row.status).toBe('delayed');
      expect(row.attempts_made).toBe(0);
      expect(row.lock_token).toBeNull();
      finish();
      await startP;
      row = await job(engine, id);
      expect(row.status).toBe('delayed'); // fenced: the late completion cannot complete a released claim
    }
  });

  test('pause marker appearing mid-run: nothing new is claimed and the wall clock still ends the run', async () => {
    const home = mkdtempSync(join(tmpdir(), 'gbrain-drain-midpause-'));
    try {
      await withEnv({ GBRAIN_HOME: home }, async () => {
        for (const engine of engines) {
          await fresh(engine);
          const a = await factsJob(engine, 1);
          const b = await factsJob(engine, 2);
          const pausing: MinionHandler = async () => {
            const marker = autopilotPausedMarkerPath();
            mkdirSync(dirname(marker), { recursive: true });
            writeFileSync(marker, 'paused mid-run by test');
            return { ok: true };
          };
          try {
            const report = await runDrain(engine, opts({ maxRuntimeMs: 800 }), { ...FAST, registerHandlers: stubHandlers(pausing) });
            expect(report.outcome).toBe('time_limit');
            expect(report.attempted).toBe(1);
            const states = [await job(engine, a), await job(engine, b)];
            expect(states.map(s => s.status).sort()).toEqual(['completed', 'waiting']);
          } finally { rmSync(autopilotPausedMarkerPath(), { force: true }); }
        }
      });
    } finally { rmSync(home, { recursive: true, force: true }); }
  });

  test('provider halt: the deferral burns no attempt and stops the drain before sweeping the rest', async () => {
    for (const engine of engines) {
      await fresh(engine);
      const ids = [await factsJob(engine, 1), await factsJob(engine, 2), await factsJob(engine, 3)];
      const halting: MinionHandler = async () => { throw new RateLeaseUnavailableError('global-llm-halt:auth:test', 1, 1, 60_000); };
      const report = await runDrain(engine, opts(), { ...FAST, registerHandlers: stubHandlers(halting) });
      expect(report.outcome).toBe('provider_halt');
      expect(report.attempted).toBe(1);
      expect(report.durable.deferred).toBe(1);
      expect(report.handler_threw).toBe(1);
      const states = await Promise.all(ids.map(i => job(engine, i)));
      expect(states.filter(s => s.status === 'delayed').length).toBe(1);
      expect(states.filter(s => s.status === 'waiting' && s.attempts_started === 0).length).toBe(2);
      expect(states.every(s => s.attempts_made === 0)).toBe(true);
    }
  });
});

describe('cost bound', () => {
  test('ALS reaches the worker handler: priced chat spend is recorded on the drain tracker', async () => {
    for (const engine of engines) {
      await fresh(engine);
      chatStub();
      await factsJob(engine, 1);
      const report = await runDrain(engine, opts({ maxUsd: 5 }), FAST);
      expect(report.outcome).toBe('empty');
      expect(chatCalls).toBe(1);
      expect(report.facts_inserted).toBe(1);
      expect(report.budget.spent_usd).toBeGreaterThan(0);
      expect(report.budget.exhausted).toBe(false);
    }
  });

  test('a cap below one reservation spends nothing and leaves later jobs unclaimed', async () => {
    for (const engine of engines) {
      await fresh(engine);
      chatStub();
      const a = await factsJob(engine, 1);
      const b = await factsJob(engine, 2);
      const report = await runDrain(engine, opts({ maxUsd: 0.0001 }), FAST);
      expect(chatCalls).toBe(0);
      expect(report.outcome).toBe('budget_exhausted');
      expect(report.budget.exhausted).toBe(true);
      expect(report.budget.spent_usd).toBe(0);
      expect(report.attempted).toBe(1);
      const states = [await job(engine, a), await job(engine, b)];
      expect(states.filter(s => s.status === 'waiting' && s.attempts_started === 0).length).toBe(1);
    }
  });

  test('unpriced configured extraction model fails closed before any claim', async () => {
    for (const engine of engines) {
      await fresh(engine);
      chatStub();
      await engine.setConfig('facts.extraction_model', 'unpricedexample:model-x');
      await factsJob(engine, 1);
      const before = await allRows(engine);
      const preview = await previewDrain(engine, previewOpts());
      expect(preview.pricing.chat).toBe('unpriced');
      expect(preview.blockers).toContain('unpriced_chat_model');
      const report = await runDrain(engine, opts(), FAST);
      expect(report.outcome).toBe('blocked');
      expect(chatCalls).toBe(0);
      expect(await allRows(engine)).toEqual(before);
    }
  });

  test('an unpriced per-job model override is refused by the tracker before the transport runs', async () => {
    for (const engine of engines) {
      await fresh(engine);
      chatStub();
      await factsJob(engine, 1, { data: { model: 'unpricedexample:model-y' } });
      const report = await runDrain(engine, opts(), FAST);
      expect(chatCalls).toBe(0);
      expect(report.outcome).toBe('budget_exhausted');
      expect(report.budget.spent_usd).toBe(0);
    }
  });

  test('embedding calls are charged to the same tracker (audit shows chat and embed records)', async () => {
    const audit = join(mkdtempSync(join(tmpdir(), 'gbrain-drain-audit-')), 'budget.jsonl');
    try {
      await withEnv({ OPENAI_API_KEY: 'sk-synthetic-drain-test' }, async () => {
        for (const engine of engines) {
          await fresh(engine);
          configureGateway({ embedding_model: 'openai:text-embedding-3-small', embedding_dimensions: 1536, env: { OPENAI_API_KEY: 'sk-synthetic-drain-test' } });
          chatStub();
          let embedCalls = 0;
          __setEmbedTransportForTests((async ({ values }: { values: string[] }) => {
            embedCalls++;
            return { embeddings: values.map(() => Array.from({ length: 1536 }, () => 0.1)), usage: { tokens: 10 } };
          }) as never);
          await factsJob(engine, 1);
          const report = await runDrain(engine, opts({ maxUsd: 5 }), { ...FAST, budgetAuditPath: audit });
          expect(report.outcome).toBe('empty');
          expect(embedCalls).toBeGreaterThan(0);
          const lines = readFileSync(audit, 'utf8').trim().split('\n').map(l => JSON.parse(l) as { event: string; kind: string; label: string });
          const kinds = new Set(lines.filter(l => l.label === 'jobs-drain:facts-absorb' && l.event.startsWith('re')).map(l => l.kind));
          expect(kinds.has('chat')).toBe(true);
          expect(kinds.has('embed')).toBe(true);
          rmSync(audit, { force: true });
        }
      });
    } finally { if (existsSync(dirname(audit))) rmSync(dirname(audit), { recursive: true, force: true }); }
  });
});

describe('deadline cancels running domain work; outcomes are durable, not optimistic', () => {
  const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
  async function marker(engine: BrainEngine, id: number): Promise<string | null> {
    return engine.getConfig(`drain.test.marker.${id}`);
  }

  test('abort-aware handler: the wall clock aborts it, no domain write lands after return, no attempt burned', async () => {
    for (const engine of engines) {
      await fresh(engine);
      const id = await factsJob(engine, 1);
      let sawAbort = false;
      const cooperative: MinionHandler = async (job) => {
        await new Promise<void>((resolve) => {
          const t = setTimeout(resolve, 1_500);
          job.signal.addEventListener('abort', () => { clearTimeout(t); sawAbort = true; resolve(); }, { once: true });
        });
        if (job.signal.aborted) throw job.signal.reason;
        await engine.setConfig(`drain.test.marker.${job.id}`, 'late-domain-write');
        return { wrote: true };
      };
      const t0 = Date.now();
      const report = await runDrain(engine, opts({ maxRuntimeMs: 300 }), { ...FAST, graceMs: 3_000, registerHandlers: stubHandlers(cooperative) });
      expect(Date.now() - t0).toBeLessThan(300 + 3_000);
      expect(sawAbort).toBe(true);
      expect(report.outcome).toBe('time_limit');
      expect(report.domain_stop_confirmed).toBe(true);
      expect(report.durable.released).toBe(1);
      await sleep(1_800); // past the handler's own 1.5s: a non-aborted handler would have written by now
      expect(await marker(engine, id)).toBeNull();
      const row = await job(engine, id);
      expect(row.status).toBe('delayed');
      expect(row.attempts_made).toBe(0);
    }
  });

  test('official facts-absorb handler: the deadline aborts the in-flight chat; no facts land later; spend is settled', async () => {
    for (const engine of engines) {
      await fresh(engine);
      let started = 0;
      let finished = 0;
      __setChatTransportForTests(async (o): Promise<ChatResult> => {
        started++;
        const signal = (o as { abortSignal?: AbortSignal }).abortSignal;
        await new Promise<void>((resolve, reject) => {
          const t = setTimeout(resolve, 1_500);
          signal?.addEventListener('abort', () => { clearTimeout(t); reject(signal.reason ?? new Error('aborted')); }, { once: true });
        });
        finished++;
        return {
          text: JSON.stringify({ facts: [{ fact: 'synthetic late fact', kind: 'event', entity: null, confidence: 1, notability: 'high' }] }),
          blocks: [], stopReason: 'end',
          usage: { input_tokens: 1000, output_tokens: 200, cache_read_tokens: 0, cache_creation_tokens: 0 },
          model: (o as { model?: string }).model ?? 'test:stub', providerId: 'test',
        };
      });
      const id = await factsJob(engine, 1);
      const report = await runDrain(engine, opts({ maxRuntimeMs: 400, maxUsd: 5 }), { ...FAST, graceMs: 3_000 });
      expect(started).toBe(1);
      await sleep(1_800);
      const facts = await engine.executeRaw<{ n: number }>(`SELECT count(*)::int AS n FROM facts WHERE fact = 'synthetic late fact'`);
      expect(Number(facts[0].n)).toBe(0);
      expect(finished).toBe(0);
      expect(report.outcome).toBe('time_limit');
      expect(report.domain_stop_confirmed).toBe(true);
      expect(report.budget.final).toBe(true);
      const row = await job(engine, id);
      expect(row.status).toBe('delayed');
      expect(row.attempts_made).toBe(0);
    }
  });

  test('a handler return is not a completion: lease loss before the terminal write is reported as dropped', async () => {
    for (const engine of engines) {
      await fresh(engine);
      const id = await factsJob(engine, 1);
      const losesLease: MinionHandler = async (job) => {
        // Another worker reclaimed the row (stall sweep + new claim) while we ran.
        await engine.executeRaw(`UPDATE minion_jobs SET lock_token = 'foreign-worker:2', claim_generation = claim_generation + 1 WHERE id = $1`, [job.id]);
        return { ok: true };
      };
      const report = await runDrain(engine, opts({ maxJobs: 1 }), { ...FAST, registerHandlers: stubHandlers(losesLease) });
      // Reported completions must be durable; before this contract `completed` counted handler returns.
      expect((report as unknown as { durable?: { completed: number }; completed?: number }).durable?.completed
        ?? (report as unknown as { completed?: number }).completed).toBe(0);
      expect(report.handler_returned).toBe(1);
      expect(report.durable.completed).toBe(0);
      expect(report.durable.dropped).toBe(1);
      expect((await job(engine, id)).status).toBe('active');
    }
  });

  test('a handler that swallows the release abort and returns is released, never completed', async () => {
    for (const engine of engines) {
      await fresh(engine);
      const id = await factsJob(engine, 1);
      const swallows: MinionHandler = async () => { await new Promise(r => setTimeout(r, 600)); return { ok: true }; };
      const report = await runDrain(engine, opts({ maxRuntimeMs: 200 }), { ...FAST, graceMs: 3_000, registerHandlers: stubHandlers(swallows) });
      expect(report.outcome).toBe('time_limit');
      expect(report.handler_returned).toBe(1);
      expect(report.durable.completed).toBe(0);
      expect(report.durable.released).toBe(1);
      expect(report.domain_stop_confirmed).toBe(true);
      const row = await job(engine, id);
      expect(row.status).toBe('delayed');
      expect(row.attempts_made).toBe(0);
    }
  });

  test('happy max-jobs completion is not aborted and is durably confirmed', async () => {
    for (const engine of engines) {
      await fresh(engine);
      const id = await factsJob(engine, 1);
      await factsJob(engine, 2);
      let aborted = false;
      const slowOk: MinionHandler = async (job) => {
        await new Promise(r => setTimeout(r, 300));
        aborted = job.signal.aborted;
        return { ok: true };
      };
      const report = await runDrain(engine, opts({ maxJobs: 1 }), { ...FAST, registerHandlers: stubHandlers(slowOk) });
      expect(aborted).toBe(false);
      expect(report.outcome).toBe('max_jobs');
      expect(report.durable.completed).toBe(1);
      expect((await job(engine, id)).status).toBe('completed');
    }
  });

  test('uncooperative live-timer handler: fails closed as unconfirmed, claim released, spend not final', async () => {
    for (const engine of engines) {
      await fresh(engine);
      const id = await factsJob(engine, 1);
      let ticker: ReturnType<typeof setInterval> | undefined;
      let release: () => void = () => {};
      const ignoresAbort: MinionHandler = () => new Promise(resolve => {
        ticker = setInterval(() => { /* live timer keeps the event loop busy; abort ignored */ }, 20);
        release = () => { clearInterval(ticker); resolve({ late: true }); };
      });
      let settled: Promise<void> | undefined;
      const t0 = Date.now();
      const report = await runDrain(engine, opts({ maxRuntimeMs: 300 }), {
        ...FAST, graceMs: 300, registerHandlers: stubHandlers(ignoresAbort), onStarted: p => { settled = p; },
      });
      expect(Date.now() - t0).toBeLessThan(300 + 300 + 3_000);
      expect(report.outcome).toBe('time_limit');
      expect(report.domain_stop_confirmed).toBe(false);
      expect(report.in_flight_unconfirmed).toBe(1);
      expect(report.budget.final).toBe(false);
      const row = await job(engine, id);
      expect(row.status).toBe('delayed');
      expect(row.attempts_made).toBe(0);
      release();
      await settled;
      expect((await job(engine, id)).status).toBe('delayed'); // late return cannot complete a released claim
    }
  });
});

describe('round 3: settlement, bounded retry promotion, chat availability', () => {
  const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
  async function setDelayed(engine: BrainEngine, id: number, due: boolean) {
    await engine.executeRaw(`UPDATE minion_jobs SET status = 'delayed', delay_until = now() ${due ? '-' : '+'} interval '10 minutes' WHERE id = $1`, [id]);
  }
  async function makeDue(engine: BrainEngine, id: number) {
    await engine.executeRaw(`UPDATE minion_jobs SET delay_until = now() - interval '1 second' WHERE id = $1`, [id]);
  }

  test('max-jobs: the final permitted handler is awaited past the worker shutdown grace, never reported early', async () => {
    for (const engine of engines) {
      await fresh(engine);
      const id = await factsJob(engine, 1);
      await factsJob(engine, 2);
      let finished = false;
      const slow: MinionHandler = async () => { await sleep(900); finished = true; return { ok: true }; };
      const report = await runDrain(engine, opts({ maxJobs: 1, maxRuntimeMs: 10_000 }), {
        ...FAST, workerShutdownGraceMs: 150, registerHandlers: stubHandlers(slow),
      });
      expect(finished).toBe(true);
      expect(report.outcome).toBe('max_jobs');
      expect(report.durable.completed).toBe(1);
      expect(report.domain_stop_confirmed).toBe(true);
      expect((await job(engine, id)).status).toBe('completed');
      expect(report.attempted).toBe(1);
    }
  });

  test('budget stop while a handler still runs: return waits for the handler, not for worker.start', async () => {
    for (const engine of engines) {
      await fresh(engine);
      await factsJob(engine, 1);
      let finished = false;
      const spendsThenWorks: MinionHandler = async () => {
        try { await chat({ messages: [{ role: 'user', content: 'synthetic' }], maxTokens: 4000 }); } catch { /* BudgetExhausted */ }
        await sleep(900);
        finished = true;
        return { ok: true };
      };
      const report = await runDrain(engine, opts({ maxUsd: 0.0001, maxRuntimeMs: 10_000 }), {
        ...FAST, workerShutdownGraceMs: 150, registerHandlers: stubHandlers(spendsThenWorks),
      });
      expect(finished).toBe(true);
      expect(report.outcome).toBe('budget_exhausted');
      expect(report.domain_stop_confirmed).toBe(true);
      expect(report.budget.final).toBe(true);
    }
  });

  test('preview counts due/future delayed and expired leases read-only; would_promote is bounded', async () => {
    for (const engine of engines) {
      await fresh(engine);
      for (const n of [1, 2, 3]) await setDelayed(engine, await factsJob(engine, n), true);
      await setDelayed(engine, await factsJob(engine, 4), false);
      await factsJob(engine, 5, { status: 'active-stalled' });
      await seedUnrelated(engine);
      const before = await allRows(engine);
      const preview = await previewDrain(engine, previewOpts({ maxJobs: 2 }));
      expect(await allRows(engine)).toEqual(before);
      expect(preview.counts).toEqual({ waiting: 0, delayed: 4, delayed_due: 3, delayed_future: 1, active: 1, active_expired: 1 });
      expect(preview.would_promote_at_most).toBe(2);
    }
  });

  test('execution promotes only due delayed facts-absorb/default, at most the run allowance; everything else byte-identical', async () => {
    for (const engine of engines) {
      await fresh(engine);
      await engine.setConfig('facts.extraction_enabled', 'false');
      const due = [await factsJob(engine, 1), await factsJob(engine, 2), await factsJob(engine, 3)];
      for (const id of due) await setDelayed(engine, id, true);
      const future = await factsJob(engine, 4); await setDelayed(engine, future, false);
      const expired = await factsJob(engine, 5, { status: 'active-stalled' });
      const dead = await factsJob(engine, 6); await engine.executeRaw(`UPDATE minion_jobs SET status = 'dead' WHERE id = $1`, [dead]);
      const cancelled = await factsJob(engine, 7, { status: 'cancelled' });
      const dueSync = await seedJob(engine, 'sync'); await setDelayed(engine, dueSync, true);
      const dueOther = await seedJob(engine, 'facts-absorb', { queue: 'other-queue', data: { slug: 'meetings/elsewhere' } }); await setDelayed(engine, dueOther, true);
      const untouched = [future, expired, dead, cancelled, dueSync, dueOther, ...(await seedUnrelated(engine))];
      const before = await rowsById(engine, untouched);
      const report = await runDrain(engine, opts({ maxJobs: 2 }), FAST);
      expect(report.attempted).toBe(2);
      expect(report.durable.completed).toBe(2);
      expect(report.promoted).toBe(2);
      expect(report.outcome).toBe('max_jobs');
      expect(await rowsById(engine, untouched)).toEqual(before);
      const states = await Promise.all(due.map(id => job(engine, id)));
      expect(states.filter(s => s.status === 'completed').length).toBe(2);
      expect(states.filter(s => s.status === 'delayed').length).toBe(1);
    }
  });

  test('promotion bound is --max-jobs independent of waiting; attempts stay <= --max-jobs; unreached promoted rows stay waiting', async () => {
    for (const engine of engines) {
      await fresh(engine);
      await engine.setConfig('facts.extraction_enabled', 'false');
      await factsJob(engine, 1); await factsJob(engine, 2);
      for (const n of [3, 4]) await setDelayed(engine, await factsJob(engine, n), true);
      const preview = await previewDrain(engine, previewOpts({ maxJobs: 3 }));
      const report = await runDrain(engine, opts({ maxJobs: 3 }), FAST);
      expect(report.attempted).toBe(3);
      expect(report.promoted).toBe(2);
      expect(preview.would_promote_at_most).toBe(2);
      expect(preview.would_attempt_at_most).toBe(3);
      expect(report.remaining.waiting).toBe(1);
      expect(report.remaining.delayed).toBe(0);
    }
  });

  test('blocked runs never promote: legacy, pause, chat-unavailable and pricing gates leave due rows untouched', async () => {
    for (const engine of engines) {
      await fresh(engine);
      const id = await factsJob(engine, 1); await setDelayed(engine, id, true);
      const legacy = await seedJob(engine, 'sync');
      await engine.executeRaw('UPDATE minion_jobs SET submission_authority = NULL WHERE id = $1', [legacy]);
      const before = await allRows(engine);
      const report = await runDrain(engine, opts(), FAST);
      expect(report.outcome).toBe('blocked');
      expect(await allRows(engine)).toEqual(before);
      expect(report.promoted).toBe(0);
    }
    const home = mkdtempSync(join(tmpdir(), 'gbrain-drain-gates-'));
    try {
      await withEnv({ GBRAIN_HOME: home }, async () => {
        const gates: Array<[string, (e: BrainEngine) => Promise<void>, () => void]> = [
          ['paused', async () => { const m = autopilotPausedMarkerPath(); mkdirSync(dirname(m), { recursive: true }); writeFileSync(m, 'paused'); },
            () => rmSync(autopilotPausedMarkerPath(), { force: true })],
          ['chat_unavailable', async () => { __setChatTransportForTests(null); }, () => {}],
          ['unpriced_chat_model', async (e) => { await e.setConfig('facts.extraction_model', 'unpricedexample:model-z'); }, () => {}],
        ];
        for (const [code, arm, disarm] of gates) {
          for (const engine of engines) {
            await fresh(engine);
            const id = await factsJob(engine, 1); await setDelayed(engine, id, true);
            await arm(engine);
            try {
              const before = await allRows(engine);
              const report = await runDrain(engine, opts(), FAST);
              expect(report.blockers).toContain(code);
              expect(await allRows(engine)).toEqual(before);
              expect(report.promoted).toBe(0);
            } finally { disarm(); }
          }
        }
      });
    } finally { rmSync(home, { recursive: true, force: true }); }
  });

  test('retry round trip: a provider failure schedules a retry; a later run promotes it once due and completes it', async () => {
    for (const engine of engines) {
      await fresh(engine);
      __setChatTransportForTests(async () => { throw new Error('synthetic provider outage'); });
      const id = await factsJob(engine, 1);
      const first = await runDrain(engine, opts({ maxJobs: 1 }), FAST);
      expect(first.durable.retry_scheduled).toBe(1);
      let row = await job(engine, id);
      expect(row.status).toBe('delayed');
      expect(row.attempts_made).toBe(1);
      const stillFuture = await runDrain(engine, opts({ maxJobs: 1 }), FAST);
      expect(stillFuture.outcome).toBe('delayed_only');
      await makeDue(engine, id);
      chatStub();
      const second = await runDrain(engine, opts({ maxJobs: 1, maxUsd: 5 }), FAST);
      expect(second.durable.completed).toBe(1);
      expect(second.promoted).toBe(1);
      expect(stillFuture.promoted).toBe(0);
      expect(second.facts_inserted).toBe(1);
      row = await job(engine, id);
      expect(row.status).toBe('completed');
      expect(row.attempts_made).toBe(1);
    }
  });

  test('self-released claims round trip: a time-limited release is promoted by a later run without burning attempts', async () => {
    for (const engine of engines) {
      await fresh(engine);
      await engine.setConfig('facts.extraction_enabled', 'false');
      const id = await factsJob(engine, 1);
      const cooperative: MinionHandler = (j) => new Promise((_, reject) => j.signal.addEventListener('abort', () => reject(j.signal.reason), { once: true }));
      const first = await runDrain(engine, opts({ maxRuntimeMs: 300 }), { ...FAST, graceMs: 2_000, registerHandlers: stubHandlers(cooperative) });
      expect(first.durable.released).toBe(1);
      await makeDue(engine, id);
      const second = await runDrain(engine, opts(), FAST);
      expect(second.durable.completed).toBe(1);
      expect(second.promoted).toBe(1);
      const row = await job(engine, id);
      expect(row.status).toBe('completed');
      expect(row.attempts_made).toBe(0);
    }
  });

  test('keyless: extraction intended but chat unavailable blocks before any claim (metadata only)', async () => {
    for (const engine of engines) {
      await fresh(engine);
      __setChatTransportForTests(null);
      await factsJob(engine, 1);
      const before = await allRows(engine);
      const preview = await previewDrain(engine, previewOpts());
      expect(preview.blockers).toContain('chat_unavailable');
      expect(preview.extraction_enabled).toBe(true);
      const report = await runDrain(engine, opts(), FAST);
      expect(report.outcome).toBe('blocked');
      expect(report.blockers).toContain('chat_unavailable');
      expect(report.attempted).toBe(0);
      expect(await allRows(engine)).toEqual(before);
    }
  });

  test('chat becomes unavailable after preview: the claim is deferred without an attempt, never consumed as a skip', async () => {
    for (const engine of engines) {
      await fresh(engine);
      const id = await factsJob(engine, 1);
      await factsJob(engine, 2);
      const losesChat: DrainDeps['registerHandlers'] = async (worker, eng) => {
        const { registerBuiltinHandlers } = await import('../src/commands/jobs.ts');
        await registerBuiltinHandlers(worker, eng, { quiet: true });
        const real = worker.getHandler('facts-absorb')!;
        worker.register('facts-absorb', async (j) => { __setChatTransportForTests(null); return real(j); });
      };
      const report = await runDrain(engine, opts(), { ...FAST, registerHandlers: losesChat });
      expect(report.outcome).toBe('chat_unavailable');
      expect(report.durable.completed).toBe(0);
      expect(report.durable.deferred).toBe(1);
      expect(report.attempted).toBe(1);
      const row = await job(engine, id);
      expect(row.status).toBe('delayed');
      expect(row.attempts_made).toBe(0);
    }
  });

  test('disabled extraction policy is explicit in the execution report', async () => {
    for (const engine of engines) {
      await fresh(engine);
      await engine.setConfig('facts.extraction_enabled', 'false');
      await factsJob(engine, 1);
      const report = await runDrain(engine, opts(), FAST);
      expect(report.extraction_enabled).toBe(false);
      expect(report.skipped).toEqual({ extraction_disabled: 1 });
    }
  });
});

describe('round 4: an older due retry is not starved by steady fresh work', () => {
  test('waiting >= maxJobs every run: the older due retry is served, attempts stay bounded, unrelated rows untouched', async () => {
    for (const engine of engines) {
      await fresh(engine);
      await engine.setConfig('facts.extraction_enabled', 'false');
      const retry = await factsJob(engine, 100);
      await engine.executeRaw(
        `UPDATE minion_jobs SET status = 'delayed', delay_until = now() - interval '1 minute',
           attempts_started = 1, attempts_made = 1, created_at = now() - interval '1 day' WHERE id = $1`, [retry]);
      const future = await factsJob(engine, 200);
      await engine.executeRaw(`UPDATE minion_jobs SET status = 'delayed', delay_until = now() + interval '1 day' WHERE id = $1`, [future]);
      const unrelated = [future, ...(await seedUnrelated(engine))];
      const before = await rowsById(engine, unrelated);
      let n = 1;
      for (let run = 1; run <= 3; run++) {
        // The observed producer cadence: a fresh batch of maxJobs equal-priority jobs per run.
        for (let k = 0; k < 4; k++) await factsJob(engine, n++);
        const report = await runDrain(engine, opts({ maxJobs: 4 }), FAST);
        expect(report.attempted).toBeLessThanOrEqual(4);
        expect(report.promoted).toBeLessThanOrEqual(4);
        expect(await rowsById(engine, unrelated)).toEqual(before);
      }
      const row = await job(engine, retry);
      expect(row.status).toBe('completed');
      expect(row.attempts_made).toBe(1);
    }
  });
});

describe('round 5: required embedding availability fails closed before any claim', () => {
  function noEmbedding(kind: 'unconfigured' | 'missing_key') {
    __setEmbedTransportForTests(null);
    if (kind === 'unconfigured') resetGateway();
    else configureGateway({ embedding_model: 'openai:text-embedding-3-small', embedding_dimensions: 1536, env: {} });
    chatStub(); // chat stays available: only the embedding is missing
  }

  for (const kind of ['unconfigured', 'missing_key'] as const) {
    test(`${kind}: preview blocks with embedding_unavailable (metadata only)`, async () => {
      for (const engine of engines) {
        await fresh(engine);
        noEmbedding(kind);
        await factsJob(engine, 1);
        const before = await allRows(engine);
        const preview = await previewDrain(engine, previewOpts());
        expect(await allRows(engine)).toEqual(before);
        expect(preview.blockers).toContain('embedding_unavailable');
        expect(preview.embedding).toBe('unavailable');
        expect(preview.pricing.embedding).toBe('unavailable');
        expect(preview.would_attempt_at_most).toBe(0);
      }
    });
  }

  test('--yes with required embedding unavailable: blocked, no attempts; repeated runs retain every row', async () => {
    for (const engine of engines) {
      await fresh(engine);
      noEmbedding('unconfigured');
      const ids = [await factsJob(engine, 1), await factsJob(engine, 2)];
      const due = await factsJob(engine, 3);
      await engine.executeRaw(`UPDATE minion_jobs SET status = 'delayed', delay_until = now() - interval '1 minute' WHERE id = $1`, [due]);
      const before = await allRows(engine);
      for (let run = 0; run < 3; run++) {
        const report = await runDrain(engine, opts(), FAST);
        expect(report.attempted).toBe(0);
        expect(report.promoted).toBe(0);
        expect(report.outcome).toBe('blocked');
        expect(report.blockers).toContain('embedding_unavailable');
      }
      expect(await allRows(engine)).toEqual(before);
      for (const id of ids) expect((await job(engine, id)).attempts_made).toBe(0);
    }
  });

  test('explicitly disabled embeddings are an explicit policy, not a blocker', async () => {
    for (const engine of engines) {
      await fresh(engine);
      noEmbedding('unconfigured');
      await engine.setConfig('embedding_disabled', 'true');
      try {
        await factsJob(engine, 1);
        const preview = await previewDrain(engine, previewOpts());
        expect(preview.embedding).toBe('disabled');
        expect(preview.blockers).not.toContain('embedding_unavailable');
      } finally {
        await engine.executeRaw(`DELETE FROM config WHERE key = 'embedding_disabled'`);
      }
    }
  });

  test('extraction disabled: embedding is not required and stays explicit', async () => {
    for (const engine of engines) {
      await fresh(engine);
      noEmbedding('unconfigured');
      await engine.setConfig('facts.extraction_enabled', 'false');
      await factsJob(engine, 1);
      const preview = await previewDrain(engine, previewOpts());
      expect(preview.extraction_enabled).toBe(false);
      expect(preview.embedding).toBe('not_required');
      expect(preview.blockers).toEqual([]);
    }
  });
});
