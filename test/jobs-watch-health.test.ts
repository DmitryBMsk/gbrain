/**
 * Jobs Watch health block — the admin dashboard's wedge / worker evidence /
 * last completion signal. The helper composes the SAME backend definitions the
 * get_job_stats op (getStats().wedge + deriveWedgeSignal) and the
 * get_status_snapshot op (supervisor pidfile + DB lock, buildQueueDepths)
 * already use, so the browser never re-derives wedge math.
 *
 * Pins the incident shape: waiting work far older than the wedge threshold,
 * zero stalled rows, no detectable worker and no completion must read as
 * wedged. Worker detection is positive-evidence only: an idle unsupervised
 * worker is invisible, so "nothing detected" must never be reported as "no
 * worker", and a failed probe must read as unknown. Synthetic job names only.
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import type { BrainEngine } from '../src/core/engine.ts';
import { dispatchToolCall } from '../src/mcp/dispatch.ts';
import { readSnapshot } from '../src/commands/jobs-watch.ts';
import { readJobsWatchHealth, type JobsWatchHealth } from '../src/commands/jobs-watch-health.ts';
import { tryAcquireDbLock } from '../src/core/db-lock.ts';
import { supervisorLockId, SUPERVISOR_LOCK_TTL_MIN } from '../src/core/minions/supervisor.ts';
import { MinionQueue } from '../src/core/minions/queue.ts';
import { MinionWorker } from '../src/core/minions/worker.ts';
import { withEnv } from './helpers/with-env.ts';

let engine: PGLiteEngine;
let scratch: string;
let pidFile: string;

const PAYLOAD_MARKER = 'synthetic-payload-marker-0b7e';
// Current queue-protocol provenance required by the minion_jobs insert trigger.
const AUTHORITY = `'{"version":1,"kind":"application"}'::jsonb`;
const NOTHING_DETECTED = {
  supervisor_pidfile: 'not_detected',
  supervisor_db_lock: 'not_detected',
  active_lease: 'not_detected',
} as const;

beforeAll(async () => {
  engine = new PGLiteEngine();
  await engine.connect({});
  await engine.initSchema();
  scratch = mkdtempSync(join(tmpdir(), 'gbrain-jobs-watch-health-'));
  // Absent unless a test writes it: pidfile evidence comes only from the test.
  pidFile = join(scratch, 'supervisor.pid');
}, 60_000);

afterAll(async () => {
  await engine.disconnect();
  rmSync(scratch, { recursive: true, force: true });
});

beforeEach(async () => {
  await engine.executeRaw('DELETE FROM minion_jobs');
  await engine.executeRaw(`DELETE FROM gbrain_cycle_locks WHERE id = $1`, [supervisorLockId('default')]);
  rmSync(pidFile, { recursive: true, force: true });
}, 30_000);

async function seedWaiting(count: number, minutesOld: number, queue = 'default'): Promise<void> {
  for (let i = 0; i < count; i++) {
    await engine.executeRaw(
      `INSERT INTO minion_jobs (submission_authority, name, queue, status, data, created_at, updated_at)
       VALUES (${AUTHORITY}, 'synthetic-backlog', $1, 'waiting', '{"note":"${PAYLOAD_MARKER}"}'::jsonb,
               now() - ($2::int * interval '1 minute'), now() - ($2::int * interval '1 minute'))`,
      [queue, minutesOld],
    );
  }
}

async function seedCompleted(minutesAgo: number): Promise<void> {
  await engine.executeRaw(
    `INSERT INTO minion_jobs (submission_authority, name, queue, status, attempts_made, attempts_started, started_at, finished_at, created_at, updated_at)
     VALUES (${AUTHORITY}, 'synthetic-done', 'default', 'completed', 1, 1,
             now() - ($1::int * interval '1 minute'), now() - ($1::int * interval '1 minute'),
             now() - ($1::int * interval '1 minute'), now() - ($1::int * interval '1 minute'))`,
    [minutesAgo],
  );
}

async function seedActive(lockValid: boolean): Promise<void> {
  await engine.executeRaw(
    `INSERT INTO minion_jobs (submission_authority, name, queue, status, attempts_started, started_at, lock_token, lock_until, created_at, updated_at)
     VALUES (${AUTHORITY}, 'synthetic-running', 'default', 'active', 1, now(), 'synthetic-lock-token',
             now() + ($1::int * interval '1 minute'), now(), now())`,
    [lockValid ? 5 : -5],
  );
}

/** Same engine, but reading cycle locks fails. */
function lockProbeFails(real: BrainEngine): BrainEngine {
  return new Proxy(real, {
    get(target, prop) {
      if (prop === 'readCycleLocks') return async () => { throw new Error('synthetic lock read failure'); };
      const v = Reflect.get(target, prop, target);
      return typeof v === 'function' ? v.bind(target) : v;
    },
  });
}

function ok(h: JobsWatchHealth | { error: 'unavailable' }): JobsWatchHealth {
  if ('error' in h) throw new Error('expected a health block, got unavailable');
  return h;
}

describe('readJobsWatchHealth', () => {
  test('incident shape: old waiting backlog, stalled=0, nothing detected, no completion → wedged', async () => {
    await seedWaiting(4, 1234);
    const snap = await readSnapshot(engine);
    // The legacy panels alone look calm: nothing stalled, nothing active.
    expect(snap.queue_health).toEqual({ waiting: 4, active: 0, stalled: 0 });

    const h = ok(await readJobsWatchHealth(engine, { pidFile }));
    expect(h.schema_version).toBe(1);
    expect(h.queue).toBe('default');
    expect([h.wedged, h.no_worker]).toEqual([false, true]); // 0.60 get_job_stats: waiting work with no registered worker is no_worker, not wedged
    expect(h.wedge_threshold_minutes).toBe(15);
    expect(h.waiting).toBe(4);
    expect(h.active_healthy).toBe(0);
    expect(h.oldest_waiting_minutes).toBeGreaterThanOrEqual(1233);
    expect(h.oldest_waiting_minutes).toBeLessThanOrEqual(1235);
    expect(h.last_completed_at).toBeNull();
    expect(h.minutes_since_completion).toBeNull();
    // Negative evidence is reported as such, not as a missing process.
    expect(h.worker).toEqual({ status: 'not_detected', detected_via: null, probes: NOTHING_DETECTED });
    expect(Number.isNaN(Date.parse(h.observed_at))).toBe(false);
  });

  test('an idle unsupervised worker is invisible: not_detected is not proof of absence', async () => {
    await seedWaiting(2, 1234);
    // A real bare worker on the same queue that only handles another job name:
    // alive, polling, but holding no supervisor pidfile, no DB lock, no lease.
    const worker = new MinionWorker(engine, { queue: 'default', pollInterval: 25 });
    worker.register('synthetic-idle-handler', async () => ({ handled: true }));
    const running = worker.start();
    try {
      await new Promise(r => setTimeout(r, 300));
      const h = ok(await readJobsWatchHealth(engine, { pidFile }));
      expect(h.worker.status).toBe('not_detected');
      expect(h.worker.status).not.toBe('missing');
      expect(h.worker.probes).toEqual(NOTHING_DETECTED);
      // The wedge verdict stays reliable: this worker cannot drain the backlog.
      expect([h.wedged, h.no_worker]).toEqual([false, true]); // 0.60 get_job_stats: waiting work with no registered worker is no_worker, not wedged

      // Prove the worker really was alive while undetected: it completes a job.
      const q = new MinionQueue(engine);
      const job = await q.add('synthetic-idle-handler', {});
      let status = '';
      for (let i = 0; i < 200 && status !== 'completed'; i++) {
        await new Promise(r => setTimeout(r, 25));
        status = (await q.getJob(job.id))?.status ?? '';
      }
      expect(status).toBe('completed');
    } finally {
      worker.stop();
      await running;
    }
  }, 30_000);

  test('wedge verdict and threshold match the get_job_stats op exactly', async () => {
    await seedWaiting(2, 90);
    await seedCompleted(60);
    for (const env of [undefined, '120']) {
      await withEnv({ GBRAIN_WEDGED_QUEUE_WARN_MINUTES: env }, async () => {
        const res = await dispatchToolCall(engine, 'get_job_stats', {}, { remote: true, transport: 'stdio', sourceId: 'default' });
        const op = JSON.parse(res.content[0].text);
        const h = ok(await readJobsWatchHealth(engine, { pidFile }));
        expect(h.wedged).toBe(op.wedged);
        expect(h.wedge_threshold_minutes).toBe(op.wedge_threshold_minutes);
        expect(h.active_healthy).toBe(op.wedge.active_healthy);
        expect(h.minutes_since_completion).toBe(op.wedge.minutes_since_completion);
      });
    }
    // Default threshold 15: 60 min since completion → wedged. Threshold 120 → not.
    await withEnv({ GBRAIN_WEDGED_QUEUE_WARN_MINUTES: undefined }, async () => {
      const h = ok(await readJobsWatchHealth(engine, { pidFile }));
      expect([h.wedged, h.no_worker]).toEqual([false, true]); // 0.60 get_job_stats: waiting work with no registered worker is no_worker, not wedged
    });
    await withEnv({ GBRAIN_WEDGED_QUEUE_WARN_MINUTES: '120' }, async () => {
      expect(ok(await readJobsWatchHealth(engine, { pidFile })).wedged).toBe(false);
    });
  });

  test('healthy progress: live supervisor lock + active lease + recent completion → running via db_lock', async () => {
    await seedWaiting(3, 40);
    await seedActive(true);
    await seedCompleted(2);
    const handle = await tryAcquireDbLock(engine, supervisorLockId('default'), SUPERVISOR_LOCK_TTL_MIN);
    expect(handle).not.toBeNull();
    try {
      const h = ok(await readJobsWatchHealth(engine, { pidFile }));
      expect(h.wedged).toBe(false);
      expect(h.active_healthy).toBe(1);
      expect(h.worker).toEqual({
        status: 'running',
        detected_via: 'db_lock',
        probes: { supervisor_pidfile: 'not_detected', supervisor_db_lock: 'detected', active_lease: 'detected' },
      });
      expect(h.last_completed_at).not.toBeNull();
      const ageMin = (Date.now() - Date.parse(h.last_completed_at!)) / 60_000;
      expect(ageMin).toBeGreaterThan(1);
      expect(ageMin).toBeLessThan(4);
      expect(h.minutes_since_completion).toBe(2);
    } finally {
      await handle!.release();
    }
  });

  test('a live supervisor pidfile is positive evidence', async () => {
    writeFileSync(pidFile, `${process.pid}\n`);
    const h = ok(await readJobsWatchHealth(engine, { pidFile }));
    expect(h.worker.status).toBe('running');
    expect(h.worker.detected_via).toBe('pidfile');
    expect(h.worker.probes.supervisor_pidfile).toBe('detected');
  });

  describe('supervisor pidfile: successful absence is distinct from a failed read', () => {
    test('missing pidfile: probe ran and found nothing → not_detected', async () => {
      const h = ok(await readJobsWatchHealth(engine, { pidFile }));
      expect(h.worker.probes.supervisor_pidfile).toBe('not_detected');
      expect(h.worker.status).toBe('not_detected');
    });

    test('valid pid of an exited process: probe ran, process not alive → not_detected', async () => {
      // Our own short-lived child: its pid is known dead after exit; no other process is signalled.
      const child = Bun.spawn([process.execPath, '-e', '0']);
      await child.exited;
      writeFileSync(pidFile, `${child.pid}\n`);
      const h = ok(await readJobsWatchHealth(engine, { pidFile }));
      expect(h.worker.probes.supervisor_pidfile).toBe('not_detected');
      expect(h.worker.status).toBe('not_detected');
    });

    test('malformed pidfile content: the read failed → failed, overall unknown', async () => {
      writeFileSync(pidFile, 'not-a-pid\n');
      const h = ok(await readJobsWatchHealth(engine, { pidFile }));
      expect(h.worker.probes.supervisor_pidfile).toBe('failed');
      expect(h.worker.status).toBe('unknown');
      expect(h.worker.detected_via).toBeNull();
    });

    test('unreadable pidfile (a directory at the path): failed, overall unknown', async () => {
      mkdirSync(pidFile);
      const h = ok(await readJobsWatchHealth(engine, { pidFile }));
      expect(h.worker.probes.supervisor_pidfile).toBe('failed');
      expect(h.worker.status).toBe('unknown');
    });

    test('a failed pidfile read never masks other positive evidence', async () => {
      mkdirSync(pidFile);
      await seedWaiting(1, 30);
      await seedActive(true);
      const h = ok(await readJobsWatchHealth(engine, { pidFile }));
      expect(h.worker.probes.supervisor_pidfile).toBe('failed');
      expect(h.worker.status).toBe('running');
      expect(h.worker.detected_via).toBe('active_lease');
    });
  });

  test('a live active lease proves a worker even without a supervisor pidfile or lock', async () => {
    await seedWaiting(1, 30);
    await seedActive(true);
    const h = ok(await readJobsWatchHealth(engine, { pidFile }));
    expect(h.wedged).toBe(false);
    expect(h.worker.status).toBe('running');
    expect(h.worker.detected_via).toBe('active_lease');
  });

  test('an expired-lock active row (dead worker) is not positive evidence', async () => {
    await seedWaiting(1, 30);
    await seedActive(false);
    const h = ok(await readJobsWatchHealth(engine, { pidFile }));
    expect(h.active_healthy).toBe(0);
    expect([h.wedged, h.no_worker]).toEqual([false, true]); // 0.60 get_job_stats: waiting work with no registered worker is no_worker, not wedged
    expect(h.worker).toEqual({ status: 'not_detected', detected_via: null, probes: NOTHING_DETECTED });
  });

  test('a failed lock probe with no positive evidence is unknown; the wedge verdict still stands', async () => {
    await seedWaiting(4, 1234);
    const h = ok(await readJobsWatchHealth(lockProbeFails(engine), { pidFile }));
    expect(h.worker).toEqual({
      status: 'unknown',
      detected_via: null,
      probes: { supervisor_pidfile: 'not_detected', supervisor_db_lock: 'failed', active_lease: 'not_detected' },
    });
    expect([h.wedged, h.no_worker]).toEqual([false, true]); // 0.60 get_job_stats: waiting work with no registered worker is no_worker, not wedged
    expect(h.waiting).toBe(4);
  });

  test('positive evidence wins over a failed probe', async () => {
    await seedWaiting(1, 30);
    await seedActive(true);
    const h = ok(await readJobsWatchHealth(lockProbeFails(engine), { pidFile }));
    expect(h.worker.status).toBe('running');
    expect(h.worker.detected_via).toBe('active_lease');
    expect(h.worker.probes.supervisor_db_lock).toBe('failed');
  });

  test('empty queue: not wedged, nothing waiting, nothing detected, no completion record retained', async () => {
    const h = ok(await readJobsWatchHealth(engine, { pidFile }));
    expect(h.wedged).toBe(false);
    expect(h.waiting).toBe(0);
    expect(h.oldest_waiting_minutes).toBeNull();
    expect(h.last_completed_at).toBeNull();
    expect(h.worker.status).toBe('not_detected');
  });

  test('health is scoped to the default queue: another queue backlog does not leak into it', async () => {
    await seedWaiting(2, 500, 'synthetic-other-queue');
    const h = ok(await readJobsWatchHealth(engine, { pidFile }));
    expect(h.queue).toBe('default');
    expect(h.waiting).toBe(0);
    expect(h.oldest_waiting_minutes).toBeNull();
    expect(h.wedged).toBe(false);
  });

  test('never carries job payloads or lock tokens', async () => {
    await seedWaiting(2, 100);
    await seedActive(true);
    const text = JSON.stringify(await readJobsWatchHealth(engine, { pidFile }));
    expect(text).not.toContain(PAYLOAD_MARKER);
    expect(text).not.toContain('synthetic-lock-token');
    expect(text).not.toContain('synthetic-backlog');
  });

  test('queue read failure → opaque unavailable marker, never a healthy default or raw error text', async () => {
    const broken = {
      executeRaw: async () => {
        throw new Error('synthetic-db-failure relation "minion_jobs" does not exist');
      },
    } as unknown as BrainEngine;
    const h = await readJobsWatchHealth(broken, { pidFile });
    expect(h).toEqual({ error: 'unavailable' });
  });
});
