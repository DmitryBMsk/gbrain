/**
 * Jobs Watch health block — the admin dashboard's "is waiting work actually
 * being processed?" signal. The queue panel's `stalled` count only covers
 * active rows with expired locks, so a backlog that no worker ever claims
 * reads stalled=0 and looks calm.
 *
 * Composes existing backend definitions only, so the browser never re-derives
 * wedge math and the surfaces cannot disagree:
 *   - wedge and no-worker verdicts: `deriveWedgeSignal` with queue stats and
 *     worker registry liveness, as in the get_job_stats op.
 *   - oldest waiting age: `buildQueueDepths` (get_status_snapshot queue section).
 *   - worker evidence: the same three signals `buildWorkersSnapshot` and
 *     `jobs supervisor status` read — supervisor pidfile, queue-scoped
 *     supervisor DB lock — plus a live active lease. Each probe is reported
 *     separately so a FAILED probe is distinguishable from "no evidence".
 *
 * Worker detection is positive-evidence only. An idle worker started without
 * a supervisor (bare `jobs work`, an autopilot-spawned worker under another
 * HOME) holds no lock and no lease, so it is invisible here. `not_detected`
 * therefore means "no probe found a worker". The queue verdict separately
 * uses the worker registry, which can classify waiting work as `no_worker`.
 *
 * Every field is scoped to one queue (default 'default'). No payloads, job
 * names, lock tokens or error text leave this module; a failed queue read
 * degrades to the opaque `{ error: 'unavailable' }` marker, never a healthy
 * default.
 */

import type { BrainEngine } from '../core/engine.ts';
import { MinionQueue, deriveWedgeSignal } from '../core/minions/queue.ts';
import { queueWorkerAlive } from '../core/minions/no-worker.ts';
import { readFileSync } from 'node:fs';
import { readSupervisorPid } from '../core/minions/supervisor-pid.ts';
import { DEFAULT_PID_FILE, supervisorLockId, SUPERVISOR_LOCK_TTL_MIN } from '../core/minions/supervisor.ts';
import { inspectLock, isLockHolderLive } from '../core/db-lock.ts';
import { buildQueueDepths } from './status.ts';

export type ProbeOutcome = 'detected' | 'not_detected' | 'failed';

export interface JobsWatchWorkerEvidence {
  /**
   * running: at least one probe found positive evidence.
   * unknown: nothing found AND at least one probe failed.
   * not_detected: every probe ran and found nothing — an idle unsupervised
   *   worker is still possible.
   */
  status: 'running' | 'not_detected' | 'unknown';
  /** First positive probe in ladder order (pidfile → DB lock → active lease). */
  detected_via: 'pidfile' | 'db_lock' | 'active_lease' | null;
  probes: {
    /** Supervisor pidfile resolved from the serving process's GBRAIN_HOME. */
    supervisor_pidfile: ProbeOutcome;
    /** Queue-scoped supervisor singleton lock in the database. */
    supervisor_db_lock: ProbeOutcome;
    /** A job in this queue holding an unexpired lock right now. */
    active_lease: ProbeOutcome;
  };
}

export interface JobsWatchHealth {
  schema_version: 1;
  /** Server clock when the block was computed (ISO). */
  observed_at: string;
  /** Queue every field below is scoped to. */
  queue: string;
  wedged: boolean;
  no_worker: boolean;
  wedge_threshold_minutes: number;
  private_queue: boolean;
  waiting: number;
  /** Active rows holding an unexpired lock. */
  active_healthy: number;
  oldest_waiting_minutes: number | null;
  /**
   * Latest RETAINED completed job in this queue (ISO). null means no completed
   * row is retained right now — never completed and pruned history are
   * indistinguishable, so null must not be read as "never completed".
   */
  last_completed_at: string | null;
  minutes_since_completion: number | null;
  worker: JobsWatchWorkerEvidence;
}

export type JobsWatchHealthResult = JobsWatchHealth | { error: 'unavailable' };

function isoOrNull(v: string | null): string | null {
  if (!v) return null;
  const ms = new Date(v).getTime();
  return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
}

async function probe(fn: () => boolean | Promise<boolean>): Promise<ProbeOutcome> {
  try {
    return (await fn()) ? 'detected' : 'not_detected';
  } catch {
    return 'failed';
  }
}

/**
 * Diagnostic pidfile probe. readSupervisorPid is best-effort and folds an
 * unreadable or corrupt file into "not running"; here only a MISSING file is a
 * successful absence — any other read or parse failure throws, so probe()
 * reports 'failed'. Liveness itself stays readSupervisorPid's (EPERM = alive).
 */
function supervisorPidfileAlive(pidFile: string): boolean {
  let text: string;
  try { text = readFileSync(pidFile, 'utf8'); }
  catch (e) {
    if ((e as NodeJS.ErrnoException)?.code === 'ENOENT') return false;
    throw e;
  }
  if (!/^[1-9]\d*$/.test(text.trim().split('\n')[0] ?? '')) throw new Error('invalid supervisor pidfile');
  return readSupervisorPid(pidFile).running;
}

async function readWorkerEvidence(
  engine: BrainEngine,
  queue: string,
  activeHealthy: number,
  pidFile: string,
): Promise<JobsWatchWorkerEvidence> {
  const probes = {
    supervisor_pidfile: await probe(() => supervisorPidfileAlive(pidFile)),
    supervisor_db_lock: await probe(async () => {
      const snap = await inspectLock(engine, supervisorLockId(queue));
      return snap !== null && isLockHolderLive(snap, SUPERVISOR_LOCK_TTL_MIN);
    }),
    active_lease: await probe(() => activeHealthy > 0),
  };
  const detected_via = probes.supervisor_pidfile === 'detected' ? 'pidfile'
    : probes.supervisor_db_lock === 'detected' ? 'db_lock'
      : probes.active_lease === 'detected' ? 'active_lease' : null;
  const status = detected_via !== null ? 'running'
    : Object.values(probes).includes('failed') ? 'unknown' : 'not_detected';
  return { status, detected_via, probes };
}

export async function readJobsWatchHealth(
  engine: BrainEngine,
  opts: { queue?: string; pidFile?: string } = {},
): Promise<JobsWatchHealthResult> {
  const queue = opts.queue || 'default';
  try {
    const stats = await new MinionQueue(engine).getStats({ queue });
    const verdict = deriveWedgeSignal(stats.wedge, { workerAlive: queueWorkerAlive(stats.wedge.queue) });
    const depth = (await buildQueueDepths(engine)).find(r => r.queue === queue);
    const oldestSeconds = depth?.oldest_waiting_age_seconds ?? null;
    const worker = await readWorkerEvidence(engine, queue, stats.wedge.active_healthy, opts.pidFile ?? DEFAULT_PID_FILE);
    const lastCompleted = isoOrNull(stats.wedge.last_completed_at);
    return {
      schema_version: 1,
      observed_at: new Date().toISOString(),
      queue,
      ...verdict,
      waiting: stats.wedge.waiting,
      active_healthy: stats.wedge.active_healthy,
      oldest_waiting_minutes: oldestSeconds === null ? null : Math.round(oldestSeconds / 60),
      last_completed_at: lastCompleted,
      minutes_since_completion: lastCompleted === null ? null : stats.wedge.minutes_since_completion,
      worker,
    };
  } catch {
    return { error: 'unavailable' };
  }
}
