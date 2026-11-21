/**
 * MinionWorker `queueMaintenance` — a handler-scoped worker must be able to
 * run WITHOUT the queue-global maintenance every default worker performs:
 * delayed promotion (every loop, all names/queues) and the stall / timeout /
 * wall-clock / waiting-TTL sweeps (every stalledInterval, all names/queues).
 *
 * The bounded `gbrain jobs drain` relies on this: claiming only its own name
 * is not enough if merely starting a worker requeues, dead-letters, promotes
 * or cancels unrelated rows. The characterization case pins that the default
 * worker really does touch them (so the opt-out is load-bearing), and that
 * the default stays unchanged for every existing caller.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { MinionQueue } from '../src/core/minions/queue.ts';
import { ABORT_REASON_RELEASE, MinionWorker, type JobSettled } from '../src/core/minions/worker.ts';
import type { MinionWorkerOpts } from '../src/core/minions/types.ts';
import { resetPgliteStateNarrow } from './helpers/reset-pglite.ts';

let engine: PGLiteEngine;

beforeAll(async () => {
  engine = new PGLiteEngine();
  await engine.connect({});
  await engine.initSchema();
});

afterAll(async () => {
  await engine.disconnect();
});

beforeEach(async () => {
  // Narrow: the full reset also clears config.version, which the worker's ensureSchema reads.
  await resetPgliteStateNarrow(engine, ['minion_jobs']);
});

async function seed(name: string, shape: 'delayed' | 'stalled' | 'waiting', queue = 'default'): Promise<number> {
  const job = await new MinionQueue(engine).add(name, {}, { queue });
  if (shape === 'delayed') {
    await engine.executeRaw(`UPDATE minion_jobs SET status = 'delayed', delay_until = now() - interval '1 minute' WHERE id = $1`, [job.id]);
  } else if (shape === 'stalled') {
    await engine.executeRaw(
      `UPDATE minion_jobs SET status = 'active', lock_token = 'foreign-worker:1', lock_until = now() - interval '10 minutes',
         started_at = now() - interval '20 minutes', claim_generation = claim_generation + 1, attempts_started = attempts_started + 1
       WHERE id = $1`, [job.id]);
  }
  return job.id;
}

async function rows(ids: number[]): Promise<string[]> {
  const r = await engine.executeRaw<{ r: string }>(
    `SELECT row_to_json(j)::text AS r FROM minion_jobs j WHERE id = ANY($1::int[]) ORDER BY id`, [ids]);
  return r.map(x => x.r);
}

async function runBriefly(opts: MinionWorkerOpts): Promise<void> {
  const worker = new MinionWorker(engine, { queue: 'default', pollInterval: 20, stalledInterval: 30, healthCheckInterval: 0, ...opts });
  worker.register('maintenance-probe-noop', async () => ({ ok: true }));
  const started = worker.start();
  await new Promise(r => setTimeout(r, 400));
  worker.stop();
  await started;
}

describe('MinionWorker queueMaintenance', () => {
  test('characterization: the default worker promotes and sweeps rows it cannot even claim', async () => {
    const ids = [await seed('extract', 'delayed'), await seed('backlinks', 'stalled'), await seed('extract', 'delayed', 'other-queue')];
    await runBriefly({});
    const after = await engine.executeRaw<{ id: number; status: string }>(
      'SELECT id, status FROM minion_jobs WHERE id = ANY($1::int[]) ORDER BY id', [ids]);
    expect(after.map(r => r.status)).toEqual(['waiting', 'waiting', 'waiting']);
  });

  test('queueMaintenance:false leaves every unrelated row byte-identical', async () => {
    const ids = [
      await seed('extract', 'delayed'), await seed('backlinks', 'stalled'),
      await seed('extract', 'delayed', 'other-queue'), await seed('sync', 'waiting'),
    ];
    const before = await rows(ids);
    await runBriefly({ queueMaintenance: false });
    expect(await rows(ids)).toEqual(before);
  });

  test('queueMaintenance:false still claims and completes its own registered jobs', async () => {
    const own = await new MinionQueue(engine).add('maintenance-probe-noop', {}, { queue: 'default' });
    await runBriefly({ queueMaintenance: false });
    const [row] = await engine.executeRaw<{ status: string }>('SELECT status FROM minion_jobs WHERE id = $1', [own.id]);
    expect(row.status).toBe('completed');
  });

  test('inFlightClaims exposes only id and lock token of claimed, running jobs', async () => {
    const own = await new MinionQueue(engine).add('maintenance-probe-noop', {}, { queue: 'default' });
    const worker = new MinionWorker(engine, { queue: 'default', pollInterval: 20, healthCheckInterval: 0, queueMaintenance: false });
    let release: () => void = () => {};
    let seen: ReadonlyArray<{ id: number; lockToken: string }> = [];
    worker.register('maintenance-probe-noop', async () => {
      seen = worker.inFlightClaims;
      await new Promise<void>(r => { release = r; });
      return { ok: true };
    });
    expect(worker.inFlightClaims).toEqual([]);
    const started = worker.start();
    while (seen.length === 0) await new Promise(r => setTimeout(r, 10));
    expect(seen.map(c => c.id)).toEqual([own.id]);
    const [row] = await engine.executeRaw<{ lock_token: string }>('SELECT lock_token FROM minion_jobs WHERE id = $1', [own.id]);
    expect(seen[0].lockToken).toBe(row.lock_token);
    worker.stop();
    release();
    await started;
    expect(worker.inFlightClaims).toEqual([]);
  });
});

describe('MinionWorker release + settle contract', () => {
  async function status(id: number) {
    const [row] = await engine.executeRaw<{ status: string; attempts_made: number; lock_token: string | null }>(
      'SELECT status, attempts_made, lock_token FROM minion_jobs WHERE id = $1', [id]);
    return row;
  }
  function worker() {
    const w = new MinionWorker(engine, { queue: 'default', pollInterval: 20, healthCheckInterval: 0, queueMaintenance: false });
    const settled: JobSettled[] = [];
    w.on('jobSettled', s => settled.push(s));
    return { w, settled };
  }

  test('jobSettled reports the durable terminal write: completed, retry_scheduled, dropped', async () => {
    const q = new MinionQueue(engine);
    const ok = await q.add('settle-ok', {}, {});
    const bad = await q.add('settle-throw', {}, { max_attempts: 3 });
    const lost = await q.add('settle-lost', {}, {});
    const { w, settled } = worker();
    w.register('settle-ok', async () => ({ ok: true }));
    w.register('settle-throw', async () => { throw new Error('synthetic failure'); });
    w.register('settle-lost', async (job) => {
      await engine.executeRaw(`UPDATE minion_jobs SET lock_token = 'foreign-worker:9', claim_generation = claim_generation + 1 WHERE id = $1`, [job.id]);
      return { ok: true };
    });
    const started = w.start();
    while (settled.length < 3) await new Promise(r => setTimeout(r, 10));
    w.stop();
    await started;
    const by = Object.fromEntries(settled.map(s => [s.id, s.outcome]));
    expect(by[ok.id]).toBe('completed');
    expect(by[bad.id]).toBe('retry_scheduled');
    expect(by[lost.id]).toBe('dropped');
    expect((await status(lost.id)).status).toBe('active');
  });

  test('releaseInFlight aborts through job.signal and releases without burning an attempt', async () => {
    const j = await new MinionQueue(engine).add('release-probe', {}, {});
    const { w, settled } = worker();
    let reason: unknown;
    let entered = false;
    w.register('release-probe', (job) => new Promise((_, reject) => {
      entered = true;
      job.signal.addEventListener('abort', () => { reason = job.signal.reason; reject(job.signal.reason); }, { once: true });
    }));
    const started = w.start();
    while (!entered) await new Promise(r => setTimeout(r, 10));
    w.releaseInFlight();
    await started;
    expect(reason instanceof Error && reason.message).toBe(ABORT_REASON_RELEASE);
    expect(settled.map(s => s.outcome)).toEqual(['released']);
    const row = await status(j.id);
    expect(row.status).toBe('delayed');
    expect(row.attempts_made).toBe(0);
    expect(row.lock_token).toBeNull();
  });

  test('a handler that swallows the release abort and returns is released, not completed', async () => {
    const j = await new MinionQueue(engine).add('release-swallow', {}, {});
    const { w, settled } = worker();
    let entered = false;
    w.register('release-swallow', async () => { entered = true; await new Promise(r => setTimeout(r, 200)); return { ok: true }; });
    const started = w.start();
    while (!entered) await new Promise(r => setTimeout(r, 10));
    w.releaseInFlight();
    await started;
    expect(settled.map(s => s.outcome)).toEqual(['released']);
    expect((await status(j.id)).status).toBe('delayed');
  });
});

describe('MinionQueue.promoteDueDelayed (scoped retry promotion)', () => {
  async function seedDelayed(name: string, due: boolean, queue = 'default', priority = 0): Promise<number> {
    const job = await new MinionQueue(engine).add(name, {}, { queue, priority });
    await engine.executeRaw(`UPDATE minion_jobs SET status = 'delayed', delay_until = now() ${due ? '-' : '+'} interval '5 minutes' WHERE id = $1`, [job.id]);
    return job.id;
  }
  async function snapshot(ids: number[]): Promise<string[]> {
    return (await engine.executeRaw<{ r: string }>(`SELECT row_to_json(j)::text AS r FROM minion_jobs j WHERE id = ANY($1::int[]) ORDER BY id`, [ids])).map(x => x.r);
  }

  test('promotes exactly `limit` due rows of one (queue, name), highest priority first; nothing else moves', async () => {
    const due: number[] = [];
    for (let i = 0; i < 10; i++) due.push(await seedDelayed('scoped-probe', true, 'default', i === 7 ? -5 : 0));
    const others = [
      await seedDelayed('scoped-probe', false),
      await seedDelayed('scoped-probe', true, 'other-queue'),
      await seedDelayed('other-name', true),
      (await new MinionQueue(engine).add('scoped-probe', {}, {})).id,
    ];
    const before = await snapshot(others);
    const ids = await new MinionQueue(engine).promoteDueDelayed({ queue: 'default', name: 'scoped-probe', limit: 3 });
    expect(ids.length).toBe(3);
    expect(ids).toContain(due[7]);
    const promoted = await engine.executeRaw<{ n: number }>(
      `SELECT count(*)::int AS n FROM minion_jobs WHERE id = ANY($1::int[]) AND status = 'waiting' AND delay_until IS NULL`, [due]);
    expect(Number(promoted[0].n)).toBe(3);
    expect(await snapshot(others)).toEqual(before);
  });

  test('limit <= 0 is a no-op; unreviewed legacy jobs refuse before any update', async () => {
    const id = await seedDelayed('scoped-probe', true);
    expect(await new MinionQueue(engine).promoteDueDelayed({ queue: 'default', name: 'scoped-probe', limit: 0 })).toEqual([]);
    const legacy = (await new MinionQueue(engine).add('sync', {}, {})).id;
    await engine.executeRaw('UPDATE minion_jobs SET submission_authority = NULL WHERE id = $1', [legacy]);
    const before = await snapshot([id]);
    await expect(new MinionQueue(engine).promoteDueDelayed({ queue: 'default', name: 'scoped-probe', limit: 5 })).rejects.toThrow();
    expect(await snapshot([id])).toEqual(before);
  });
});
