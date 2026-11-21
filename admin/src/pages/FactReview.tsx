import React, { useState } from 'react';

/**
 * Owner-only, read-only review of completed facts-absorb jobs. Loads only on an
 * explicit click for the job IDs typed in; nothing is cached or stored. Text is
 * rendered through React (escaped), never as HTML. Mirrors the response shape
 * of src/commands/admin-fact-review.ts; the source verdicts are server-computed.
 */

interface ReviewFact {
  id: number; fact: string; kind: string; entity_slug: string | null; visibility: string; notability: string;
  source: string; context: string | null; valid_from: string | null; valid_until: string | null; expired_at: string | null;
  superseded_by: number | null; confidence: number; created_at: string | null; state: 'active' | 'expired' | 'superseded';
}
/** Persistence request input: provenance only, never the extraction source. Text fields appear only when it differs. */
interface RequestInput {
  comparison: 'identical' | 'differs' | 'not_compared';
  operation: string; source_kind: string | null; source_uri: string | null; ingested_via: string | null; recorded_at: string | null;
  title?: string; type?: string; body?: string; body_truncated?: boolean;
}
interface Snapshot {
  status: string; title?: string; type?: string; body?: string; body_truncated?: boolean; recorded_at?: string | null;
  request_input?: RequestInput;
}
interface ReviewSource {
  source_id: string; slug: string | null; page_id: number | null; revision: string | null;
  persistence_request_id: string | null; request: string; snapshot: Snapshot;
}
type Receipt = { status: 'facts'; inserted: number | null; duplicate: number | null; superseded: number | null; fact_ids: number[] }
  | { status: 'skipped'; reason: string } | { status: 'invalid' };
type ReviewJob =
  | { job_id: number; status: 'not_found' | 'not_facts_absorb' }
  | { job_id: number; status: 'not_completed'; job_status: string }
  | { job_id: number; status: 'completed'; finished_at: string | null; receipt: Receipt; facts: ReviewFact[]; missing_fact_ids: number[]; source: ReviewSource };

const MAX_JOBS = 8;
const WARN = 'var(--accent-warn, #d29922)';
const DANGER = 'var(--accent-danger, #f85149)';
const OK = 'var(--accent-success, #2ea043)';
const MUTED = { color: 'var(--text-muted, #777)', fontSize: 12 };
const PRE: React.CSSProperties = { whiteSpace: 'pre-wrap', wordBreak: 'break-word', margin: 0, fontFamily: 'inherit' };

/** "12, 13 ,14" → [12,13,14]; null when not 1–8 unique positive integers. */
function parseJobIds(input: string): number[] | null {
  const compact = input.replace(/\s+/g, '');
  if (!/^[1-9][0-9]{0,9}(?:,[1-9][0-9]{0,9})*$/.test(compact)) return null;
  const ids = compact.split(',').map(Number);
  return ids.length <= MAX_JOBS && new Set(ids).size === ids.length && ids.every(id => id <= 2_147_483_647) ? ids : null;
}

const SNAPSHOT_LABEL: Record<string, { text: string; color: string }> = {
  revision_version: { text: 'Page version recorded at the extraction revision: the exact text the extraction read', color: OK },
  current_revision_match: { text: 'Current page, still at the extraction revision: the exact text the extraction read', color: OK },
  request_input_unverified: { text: 'UNVERIFIED writer input: the page text at the extraction revision is no longer retained, so this persistence request input may differ from what the extraction read', color: WARN },
  current_hash_match: { text: 'Current page, unverified: its body matches the hash recorded at submission, but no revision was recorded, so the extraction-time text is not proven', color: WARN },
  current_changed: { text: 'Current page, CHANGED since extraction: this is NOT the text the facts were extracted from', color: DANGER },
  current_unverified: { text: 'Current page, UNVERIFIED: the job recorded no revision or hash, so this may not be the extraction-time text', color: WARN },
  unavailable: { text: 'Source unavailable: the page is gone and no snapshot is retained', color: DANGER },
};
const COMPARISON: Record<string, { text: string; color: string }> = {
  identical: { text: 'Persistence request input is identical to this text.', color: OK },
  differs: { text: 'Persistence request input DIFFERS from the text the extraction read; it is shown below for provenance only and is not the extraction source.', color: WARN },
  not_compared: { text: 'Not compared: no revision-fenced page text is retained.', color: WARN },
};
const REQUEST_LABEL: Record<string, string> = {
  used: 'used', absent: 'none recorded', invalid_id: 'malformed id (ignored)', not_found: 'not found',
  mismatch: 'does not match this job (ignored)', compacted: 'input compacted', no_content: 'no page content recorded',
};

const when = (v: string | null | undefined) => (v ? new Date(v).toLocaleString() : '—');

function receiptLine(r: Receipt): { text: string; color?: string } {
  if (r.status === 'facts') {
    const n = (v: number | null) => (v === null ? '?' : String(v));
    return { text: `Receipt: ${r.fact_ids.length} fact ID${r.fact_ids.length === 1 ? '' : 's'} (inserted ${n(r.inserted)}, duplicate ${n(r.duplicate)}, superseded ${n(r.superseded)})` };
  }
  if (r.status === 'skipped') return { text: `Receipt: extraction skipped (${r.reason}); no facts`, color: WARN };
  return { text: 'Receipt unreadable: no fact IDs could be trusted, so no facts are shown', color: DANGER };
}

function FactItem({ f }: { f: ReviewFact }) {
  const stateColor = f.state === 'active' ? undefined : WARN;
  return (
    <li style={{ padding: '8px 0', borderBottom: '1px solid var(--border, #333)' }}>
      <div style={{ display: 'flex', gap: 8, alignItems: 'baseline' }}>
        <span style={MUTED}>#{f.id}</span>
        <p style={PRE}>{f.fact}</p>
      </div>
      <div style={MUTED}>
        <span style={{ color: stateColor }}>{f.state}{f.superseded_by !== null ? ` by #${f.superseded_by}` : ''}</span>
        {' · '}{f.kind} · {f.visibility} · notability {f.notability} · confidence {f.confidence}
        {' · '}entity {f.entity_slug ?? '—'} · via {f.source}
      </div>
      <div style={MUTED}>
        valid {when(f.valid_from)} → {f.valid_until ? when(f.valid_until) : 'open'}
        {f.expired_at ? ` · expired ${when(f.expired_at)}` : ''} · recorded {when(f.created_at)}
      </div>
      {f.context && <div style={MUTED}>context: {f.context}</div>}
    </li>
  );
}

function TextBlock({ title, type, at, body, truncated }: { title?: string; type?: string; at?: string | null; body: string; truncated?: boolean }) {
  return (
    <>
      <h4 style={{ margin: '8px 0 4px' }}>{title || '(untitled)'} <span style={MUTED}>{type} · {when(at)}</span></h4>
      <pre style={{ ...PRE, maxHeight: 480, overflow: 'auto', padding: 8, border: '1px solid var(--border, #333)' }}>{body}</pre>
      {truncated && <div style={{ color: WARN }}>Body truncated for display.</div>}
    </>
  );
}

function SourcePanel({ source }: { source: ReviewSource }) {
  const s = source.snapshot;
  const r = s.request_input;
  const label = SNAPSHOT_LABEL[s.status] ?? { text: `Unrecognized source status (${s.status}); treat as unverified`, color: DANGER };
  const comparison = r ? COMPARISON[r.comparison] ?? { text: 'Comparison unknown; treat the request input as unverified.', color: DANGER } : null;
  return (
    <section aria-label="Source text" style={{ flex: '1 1 0', minWidth: 0 }}>
      <div role="status" style={{ color: label.color, fontWeight: 600 }}>{label.text}</div>
      {comparison && <div style={{ color: comparison.color, marginTop: 4 }}>{comparison.text}</div>}
      <div style={{ ...MUTED, marginTop: 4 }}>
        source {source.source_id} · page {source.slug ?? '—'} (id {source.page_id ?? '—'}) · extraction revision {source.revision ?? 'not recorded'}
        {' · '}persistence request {source.persistence_request_id ?? '—'} ({REQUEST_LABEL[source.request] ?? source.request})
      </div>
      {r && <div style={MUTED}>{r.operation} · source kind {r.source_kind ?? '—'} · ingested via {r.ingested_via ?? '—'} · uri {r.source_uri ?? '—'}</div>}
      {s.body !== undefined && <TextBlock title={s.title} type={s.type} at={s.recorded_at} body={s.body} truncated={s.body_truncated} />}
      {r?.comparison === 'differs' && r.body !== undefined && (
        <section aria-label="Request input (provenance only)" style={{ marginTop: 12, opacity: 0.85 }}>
          <div style={{ color: WARN }}>Persistence request input — provenance only, NOT the extraction source</div>
          <TextBlock title={r.title} type={r.type} at={r.recorded_at} body={r.body} truncated={r.body_truncated} />
        </section>
      )}
    </section>
  );
}

function JobSection({ job }: { job: ReviewJob }) {
  let body: React.ReactNode;
  if (job.status !== 'completed') {
    body = (
      <p style={{ color: WARN }}>
        {job.status === 'not_completed' ? `Not completed (status: ${job.job_status}); nothing is shown.`
          : job.status === 'not_found' ? 'Job not found.' : 'Not a facts-absorb job on the default queue; nothing is shown.'}
      </p>
    );
  } else {
    const receipt = receiptLine(job.receipt);
    body = (
      <>
        <div style={{ color: receipt.color }}>{receipt.text} · finished {when(job.finished_at)}</div>
        {job.missing_fact_ids.length > 0 && (
          <div style={{ color: WARN }}>Receipt IDs not found in source {job.source.source_id}: {job.missing_fact_ids.map(id => `#${id}`).join(', ')}</div>
        )}
        <div style={{ display: 'flex', gap: 24, marginTop: 8, alignItems: 'flex-start' }}>
          <ul aria-label="Facts" style={{ flex: '1 1 0', minWidth: 0, listStyle: 'none', padding: 0, margin: 0 }}>
            {job.facts.map(f => <FactItem key={f.id} f={f} />)}
            {job.facts.length === 0 && <li style={MUTED}>No facts to show.</li>}
          </ul>
          <SourcePanel source={job.source} />
        </div>
      </>
    );
  }
  return (
    <section aria-labelledby={`fact-review-job-${job.job_id}`} style={{ marginTop: 24 }}>
      <h2 id={`fact-review-job-${job.job_id}`} style={{ fontSize: 14, marginBottom: 8 }}>Job {job.job_id}</h2>
      {body}
    </section>
  );
}

const ERRORS: Record<string, string> = {
  invalid_job_ids: 'The server rejected the job IDs.',
  too_many_facts: 'These jobs list more than 200 facts together; load fewer jobs at a time.',
};

export function FactReviewPage() {
  const [input, setInput] = useState('');
  const [jobs, setJobs] = useState<ReviewJob[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const load = async (event: React.FormEvent) => {
    event.preventDefault();
    const ids = parseJobIds(input);
    setJobs(null);
    if (!ids) { setError(`Enter 1–${MAX_JOBS} unique positive job IDs separated by commas.`); return; }
    setError(null);
    setLoading(true);
    try {
      const res = await fetch(`/admin/api/facts-absorb/review?job_ids=${encodeURIComponent(ids.join(','))}`, { credentials: 'same-origin' });
      if (res.status === 401) { window.location.hash = '#login'; return; }
      const body = await res.json().catch(() => null);
      if (!res.ok) { setError(ERRORS[body?.error] ?? `Fact review unavailable (HTTP ${res.status}).`); return; }
      if (body?.schema_version !== 1 || !Array.isArray(body.jobs)) { setError('Unrecognized fact review response.'); return; }
      setJobs(body.jobs as ReviewJob[]);
    } catch {
      setError('Fact review request failed.');
    } finally { setLoading(false); }
  };

  return (
    <div style={{ padding: 24 }}>
      <h1 style={{ fontSize: 18, marginBottom: 4 }}>Fact Review</h1>
      <p style={MUTED}>
        Read-only owner review of completed facts-absorb jobs: the facts each job's receipt recorded, next to the source text the extraction read.
        Nothing loads until you choose Load.
      </p>
      <form onSubmit={load} style={{ display: 'flex', gap: 8, marginTop: 12, alignItems: 'center' }}>
        <label htmlFor="fact-review-ids">Completed job IDs</label>
        <input id="fact-review-ids" value={input} onChange={e => setInput(e.target.value)} placeholder="e.g. 12, 13" autoComplete="off" style={{ minWidth: 220 }} />
        <button type="submit" disabled={loading}>{loading ? 'Loading…' : 'Load'}</button>
      </form>
      {error && <div role="alert" style={{ color: DANGER, marginTop: 12 }}>{error}</div>}
      {jobs?.map(job => <JobSection key={job.job_id} job={job} />)}
    </div>
  );
}
