import React, { useEffect, useState } from 'react';
import { api } from '../api';

/**
 * v0.41 D2 — live jobs dashboard. Browser counterpart to the TTY
 * `gbrain jobs watch` command. Polls `/admin/api/jobs/watch` every
 * 1s (matches TTY refresh cadence; SSE upgrade is a v0.42 follow-up
 * once the same wiring lands in serve-http for the TTY command).
 *
 * Layout intentionally matches the TTY 1:1 so an operator looking at
 * both surfaces sees the same panels in the same order.
 */

interface WatchSnapshot {
  ts_ms: number;
  by_type: Array<{ name: string; total: number; completed: number; failed: number; dead: number }>;
  queue_health: { waiting: number; active: number; stalled: number };
  lease_pressure_1h: number;
  top_errors: Array<{ cluster: string; count: number }>;
  budget_owners: Array<{ owner_id: number; remaining_cents: number; total_spent_cents: number }>;
  /** Server-computed, queue-scoped health block; absent on older servers. */
  health?: unknown;
}

/**
 * Mirrors `JobsWatchHealth` in src/commands/jobs-watch-health.ts. Every
 * verdict (wedged, threshold, worker status) is computed server-side from the
 * get_job_stats / get_status_snapshot definitions; this page only displays it.
 */
interface JobsHealth {
  schema_version: 1;
  observed_at: string;
  queue: string;
  wedged: boolean;
  wedge_threshold_minutes: number;
  private_queue: boolean;
  waiting: number;
  active_healthy: number;
  oldest_waiting_minutes: number | null;
  last_completed_at: string | null;
  minutes_since_completion: number | null;
  worker: {
    status: 'running' | 'not_detected' | 'unknown';
    detected_via: 'pidfile' | 'db_lock' | 'active_lease' | null;
    probes: Record<ProbeName, 'detected' | 'not_detected' | 'failed'>;
  };
}

type ProbeName = 'supervisor_pidfile' | 'supervisor_db_lock' | 'active_lease';
type HealthView = { kind: 'ok'; h: JobsHealth } | { kind: 'unknown' } | { kind: 'unavailable' };

const isNum = (v: unknown): v is number => typeof v === 'number' && Number.isFinite(v);
const isNumOrNull = (v: unknown) => v === null || isNum(v);
const isStrOrNull = (v: unknown) => v === null || typeof v === 'string';

const PROBES: Array<[ProbeName, string]> = [
  ['supervisor_pidfile', 'supervisor pidfile'],
  ['supervisor_db_lock', 'supervisor lock'],
  ['active_lease', 'active job lease'],
];
const VIA_PROBE: Record<string, ProbeName> = { pidfile: 'supervisor_pidfile', db_lock: 'supervisor_db_lock', active_lease: 'active_lease' };

/**
 * Worker evidence must be internally consistent with its probe outcomes; a
 * `running` claim without a matching detected probe (or any other mismatch)
 * is rejected rather than trusted.
 */
function validWorker(w: unknown): boolean {
  if (!w || typeof w !== 'object') return false;
  const { status, detected_via, probes } = w as Record<string, unknown>;
  if (!probes || typeof probes !== 'object') return false;
  const p = probes as Record<string, unknown>;
  const outcomes = PROBES.map(([k]) => p[k]);
  if (!outcomes.every(o => o === 'detected' || o === 'not_detected' || o === 'failed')) return false;
  if (status === 'running') return typeof detected_via === 'string' && detected_via in VIA_PROBE && p[VIA_PROBE[detected_via]] === 'detected';
  if (detected_via !== null || outcomes.includes('detected')) return false;
  if (status === 'unknown') return outcomes.includes('failed');
  if (status === 'not_detected') return outcomes.every(o => o === 'not_detected');
  return false;
}

/** Missing, legacy or malformed health is `unknown` — never rendered as healthy. */
function parseHealth(raw: unknown): HealthView {
  if (!raw || typeof raw !== 'object') return { kind: 'unknown' };
  const h = raw as Record<string, unknown>;
  if ('error' in h) return { kind: 'unavailable' };
  const valid = h.schema_version === 1 && typeof h.queue === 'string' && typeof h.observed_at === 'string'
    && typeof h.wedged === 'boolean' && typeof h.private_queue === 'boolean' && isNum(h.wedge_threshold_minutes)
    && isNum(h.waiting) && isNum(h.active_healthy) && isNumOrNull(h.oldest_waiting_minutes)
    && isStrOrNull(h.last_completed_at) && isNumOrNull(h.minutes_since_completion)
    && validWorker(h.worker);
  return valid ? { kind: 'ok', h: raw as JobsHealth } : { kind: 'unknown' };
}

function workerLabel(h: JobsHealth): string {
  const w = h.worker;
  if (w.status === 'running') return `running (${PROBES.find(([k]) => k === VIA_PROBE[w.detected_via!])![1]})`;
  if (w.status === 'unknown') {
    const failed = PROBES.filter(([k]) => w.probes[k] === 'failed').map(([, label]) => label).join(', ');
    return `unknown (detection failed: ${failed})`;
  }
  return 'not detected (an idle worker started without a supervisor leaves no trace, so this does not prove none is running)';
}

function probesLabel(h: JobsHealth): string {
  return PROBES.map(([k, label]) => `${label}: ${h.worker.probes[k].replace('_', ' ')}`).join(' · ');
}

function lastCompletionLabel(h: JobsHealth): string {
  // Retained completed rows only: "never completed" and "completions pruned" look identical.
  if (h.last_completed_at === null) return 'no completion record retained';
  const at = new Date(h.last_completed_at).toLocaleString();
  return h.minutes_since_completion === null ? at : `${h.minutes_since_completion} min ago (${at})`;
}

function oldestLabel(h: JobsHealth): string {
  if (h.waiting === 0) return 'none';
  return h.oldest_waiting_minutes === null ? 'unknown' : `${h.oldest_waiting_minutes} min`;
}

function stateLabel(h: JobsHealth): { text: string; color?: string } {
  if (h.wedged) return { text: 'wedged — waiting work is making no progress', color: 'var(--accent-danger, #f85149)' };
  if (h.private_queue) return { text: 'private parent-owned queue (no shared worker claims it)' };
  if (h.waiting === 0) return { text: 'no waiting jobs' };
  if (h.active_healthy > 0) return { text: 'processing', color: 'var(--accent-success, #2ea043)' };
  return { text: `waiting; last completion within the ${h.wedge_threshold_minutes} min threshold` };
}

const WARN = 'var(--accent-warn, #d29922)';
const DANGER = 'var(--accent-danger, #f85149)';

function WedgeAlert({ h }: { h: JobsHealth }) {
  return (
    <div role="alert" style={{ marginTop: 16, padding: 12, border: `1px solid ${DANGER}`, color: DANGER }}>
      <strong>Waiting jobs are not being processed</strong>
      <div style={{ marginTop: 4 }}>
        {h.waiting} waiting in queue {h.queue}, oldest {oldestLabel(h)}, {h.active_healthy} active now,
        last completion {lastCompletionLabel(h)} (threshold {h.wedge_threshold_minutes} min).
      </div>
      <div style={{ marginTop: 4 }}>
        Worker: {workerLabel(h)}. The stalled count only covers active jobs with expired locks, so it can
        read 0 while nothing runs. Check which jobs worker, if any, serves this queue on the brain host.
      </div>
    </div>
  );
}

function HealthPanel({ view }: { view: HealthView }) {
  const row = { margin: '2px 0' };
  let body: React.ReactNode;
  if (view.kind === 'ok') {
    const { h } = view;
    const state = stateLabel(h);
    body = (
      <>
        <div style={{ ...row, color: 'var(--text-muted, #777)', fontSize: 12 }}>
          scope: queue {h.queue} · observed {new Date(h.observed_at).toLocaleTimeString()} (server clock)
        </div>
        <div style={{ ...row, color: state.color }}>state: {state.text}</div>
        <div style={{ ...row, color: h.worker.status === 'running' ? undefined : WARN }}>worker: {workerLabel(h)}</div>
        <div style={{ ...row, color: 'var(--text-muted, #777)', fontSize: 12 }}>detection: {probesLabel(h)}</div>
        <div style={row}>active now: {h.active_healthy}</div>
        <div style={row}>oldest waiting: {oldestLabel(h)}</div>
        <div style={row}>last completion: {lastCompletionLabel(h)}</div>
        <div style={row}>wedge threshold: {h.wedge_threshold_minutes} min</div>
      </>
    );
  } else {
    body = (
      <div style={{ color: WARN }}>
        <div style={row}>
          {view.kind === 'unavailable'
            ? 'Health unavailable: the server could not compute worker and wedge health.'
            : 'Health unknown: this server did not report worker and wedge health.'}
          {' '}Do not read the other panels as healthy.
        </div>
        <div style={row}>worker: unknown</div>
        <div style={row}>last completion: unknown</div>
      </div>
    );
  }
  return (
    <section aria-labelledby="jobs-worker-health" style={{ marginTop: 24 }}>
      <h2 id="jobs-worker-health" style={{ fontSize: 14, marginBottom: 8 }}>Worker health</h2>
      {body}
    </section>
  );
}

function leasePressureColor(n: number): string {
  if (n === 0) return 'var(--accent-success, #2ea043)';
  if (n >= 100) return 'var(--accent-danger, #f85149)';
  return 'var(--accent-warn, #d29922)';
}

function dollars(cents: number): string {
  return `$${(cents / 100).toFixed(2)}`;
}

export function JobsWatchPage() {
  const [snap, setSnap] = useState<WatchSnapshot | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const tick = async () => {
      try {
        const data = await api.jobsWatch();
        if (alive) {
          setSnap(data);
          setErr(null);
        }
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
      if (alive) timer = setTimeout(tick, 1000);
    };

    tick();
    return () => {
      alive = false;
      if (timer) clearTimeout(timer);
    };
  }, []);

  if (err) {
    return (
      <div style={{ padding: 24, color: 'var(--accent-danger, #f85149)' }}>
        <h2>Jobs Watch — error</h2>
        <pre style={{ whiteSpace: 'pre-wrap' }}>{err}</pre>
      </div>
    );
  }

  if (!snap) {
    return <div style={{ padding: 24, color: 'var(--text-muted, #777)' }}>Loading jobs watch…</div>;
  }

  const ts = new Date(snap.ts_ms).toLocaleTimeString();
  const health = parseHealth(snap.health);

  return (
    <div style={{ padding: 24, fontFamily: 'var(--font-mono, "JetBrains Mono", monospace)' }}>
      <h1 style={{ fontSize: 18, marginBottom: 4 }}>
        Jobs Watch
        <span style={{ marginLeft: 12, color: 'var(--text-muted, #777)', fontSize: 12, fontWeight: 'normal' }}>
          updated {ts}
        </span>
      </h1>

      {health.kind === 'ok' && health.h.wedged && <WedgeAlert h={health.h} />}

      <section style={{ marginTop: 24 }}>
        <h2 style={{ fontSize: 14, marginBottom: 8 }}>Queue (all queues)</h2>
        <div>
          waiting=<b>{snap.queue_health.waiting}</b>{'  '}
          active=<b>{snap.queue_health.active}</b>{'  '}
          stalled=<b style={{ color: snap.queue_health.stalled > 0 ? 'var(--accent-warn, #d29922)' : undefined }}>
            {snap.queue_health.stalled}
          </b>
        </div>
      </section>

      <HealthPanel view={health} />

      {snap.by_type.length > 0 && (
        <section style={{ marginTop: 24 }}>
          <h2 style={{ fontSize: 14, marginBottom: 8 }}>By type (24h)</h2>
          <table style={{ borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ color: 'var(--text-muted, #777)', fontSize: 12 }}>
                <th style={{ textAlign: 'left', padding: '4px 12px 4px 0' }}>name</th>
                <th style={{ textAlign: 'right', padding: '4px 12px' }}>total</th>
                <th style={{ textAlign: 'right', padding: '4px 12px' }}>done</th>
                <th style={{ textAlign: 'right', padding: '4px 12px' }}>fail</th>
                <th style={{ textAlign: 'right', padding: '4px 12px' }}>dead</th>
              </tr>
            </thead>
            <tbody>
              {snap.by_type.slice(0, 6).map(t => (
                <tr key={t.name}>
                  <td style={{ padding: '4px 12px 4px 0' }}>{t.name}</td>
                  <td style={{ textAlign: 'right', padding: '4px 12px' }}>{t.total}</td>
                  <td style={{ textAlign: 'right', padding: '4px 12px' }}>{t.completed}</td>
                  <td style={{ textAlign: 'right', padding: '4px 12px' }}>{t.failed}</td>
                  <td style={{ textAlign: 'right', padding: '4px 12px' }}>{t.dead}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}

      <section style={{ marginTop: 24 }}>
        <h2 style={{ fontSize: 14, marginBottom: 8 }}>Lease pressure (1h)</h2>
        <div style={{ color: leasePressureColor(snap.lease_pressure_1h) }}>
          {snap.lease_pressure_1h} bounce{snap.lease_pressure_1h === 1 ? '' : 's'}
        </div>
      </section>

      {snap.top_errors.length > 0 && (
        <section style={{ marginTop: 24 }}>
          <h2 style={{ fontSize: 14, marginBottom: 8 }}>Top errors (24h)</h2>
          <table style={{ borderCollapse: 'collapse' }}>
            <tbody>
              {snap.top_errors.slice(0, 5).map(e => (
                <tr key={e.cluster}>
                  <td style={{ textAlign: 'right', padding: '4px 12px 4px 0', color: 'var(--text-muted, #777)' }}>
                    {e.count}×
                  </td>
                  <td style={{ padding: '4px 12px 4px 0' }}>{e.cluster}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}

      {snap.budget_owners.length > 0 && (
        <section style={{ marginTop: 24 }}>
          <h2 style={{ fontSize: 14, marginBottom: 8 }}>Budget owners</h2>
          <table style={{ borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ color: 'var(--text-muted, #777)', fontSize: 12 }}>
                <th style={{ textAlign: 'left', padding: '4px 12px 4px 0' }}>owner</th>
                <th style={{ textAlign: 'right', padding: '4px 12px' }}>spent</th>
                <th style={{ textAlign: 'right', padding: '4px 12px' }}>remaining</th>
              </tr>
            </thead>
            <tbody>
              {snap.budget_owners.slice(0, 5).map(b => (
                <tr key={b.owner_id}>
                  <td style={{ padding: '4px 12px 4px 0' }}>{b.owner_id}</td>
                  <td style={{ textAlign: 'right', padding: '4px 12px' }}>{dollars(b.total_spent_cents)}</td>
                  <td style={{ textAlign: 'right', padding: '4px 12px' }}>{dollars(b.remaining_cents)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}
    </div>
  );
}
