/**
 * Owner-only review of completed facts-absorb jobs (MEM-01). Read-only: fact ids
 * come solely from each job's structured result receipt, facts are read inside the
 * job's own source, and the primary source text is the one the extraction read —
 * the page at the job's recorded revision (current page or recorded version).
 * The persistence request's input is provenance beside it; without revision-fenced
 * text it is shown only as unverified input, and a current-page fallback is
 * explicitly marked as such.
 * Served behind requireAdmin (owner cookie); OAuth/bearer callers never reach it.
 */
import { createHash } from 'node:crypto';
import type { Request, Response } from 'express';
import type { BrainEngine } from '../core/engine.ts';
import { parseMarkdown } from '../core/markdown.ts';
import { MinionQueue } from '../core/minions/queue.ts';
import type { MinionJob, MinionJobStatus } from '../core/minions/types.ts';
import { getWriteRequestById } from '../core/persistence/journal.ts';

export const FACT_REVIEW_MAX_JOBS = 8;
export const FACT_REVIEW_MAX_FACTS = 200;
export const FACT_REVIEW_MAX_BODY_CHARS = 100_000;
const MAX_JOB_ID = 2_147_483_647; // minion_jobs.id is SERIAL
const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

export type FactReviewQuery = { ok: true; jobIds: number[] } | { ok: false; error: 'invalid_job_ids' };

/** `?job_ids=1,2,3` only: one string, unique positive ids, no other parameters. */
export function parseFactReviewQuery(query: unknown): FactReviewQuery {
  const invalid = { ok: false, error: 'invalid_job_ids' } as const;
  if (!query || typeof query !== 'object' || Array.isArray(query)) return invalid;
  const keys = Object.keys(query);
  const raw = (query as Record<string, unknown>).job_ids;
  if (keys.length !== 1 || keys[0] !== 'job_ids' || typeof raw !== 'string' || raw.length > 128) return invalid;
  if (!/^[1-9][0-9]{0,9}(?:,[1-9][0-9]{0,9})*$/.test(raw)) return invalid;
  const jobIds = raw.split(',').map(Number);
  if (jobIds.length > FACT_REVIEW_MAX_JOBS || new Set(jobIds).size !== jobIds.length || jobIds.some(id => id > MAX_JOB_ID)) return invalid;
  return { ok: true, jobIds };
}

export class FactReviewLimitError extends Error {
  readonly code = 'too_many_facts';
  constructor() { super('The selected receipts list more facts than one review may load.'); }
}

export interface ReviewFact {
  id: number; fact: string; kind: string; entity_slug: string | null; visibility: string; notability: string;
  source: string; context: string | null; valid_from: string | null; valid_until: string | null; expired_at: string | null;
  superseded_by: number | null; confidence: number; created_at: string | null; state: 'active' | 'expired' | 'superseded';
}
type Receipt = { status: 'facts'; inserted: number | null; duplicate: number | null; superseded: number | null; fact_ids: number[] }
  | { status: 'skipped'; reason: string } | { status: 'invalid' };
/**
 * current_revision_match / revision_version: the revision-fenced page.compiled_truth the handler extracted from.
 * request_input_unverified: no revision-fenced text is retained; the body is the writer's input, not proven to be what was read.
 */
export type SnapshotStatus = 'current_revision_match' | 'revision_version' | 'request_input_unverified' | 'current_hash_match'
  | 'current_changed' | 'current_unverified' | 'unavailable';
/** Provenance only, never the extraction source. `title`/`type`/`body` are present only when the input differs from the revision text. */
export interface RequestInputProvenance {
  comparison: 'identical' | 'differs' | 'not_compared';
  operation: string; source_kind: string | null; source_uri: string | null; ingested_via: string | null; recorded_at: string | null;
  title?: string; type?: string; body?: string; body_truncated?: boolean;
}
export interface SourceSnapshot {
  status: SnapshotStatus;
  title?: string; type?: string; body?: string; body_truncated?: boolean; recorded_at?: string | null;
  request_input?: RequestInputProvenance;
}
export interface ReviewSource {
  source_id: string; slug: string | null; page_id: number | null; revision: string | null; persistence_request_id: string | null;
  request: 'used' | 'absent' | 'invalid_id' | 'not_found' | 'mismatch' | 'compacted' | 'no_content';
  snapshot: SourceSnapshot;
}
export type FactReviewJob =
  | { job_id: number; status: 'not_found' | 'not_facts_absorb' }
  | { job_id: number; status: 'not_completed'; job_status: MinionJobStatus }
  | { job_id: number; status: 'completed'; finished_at: string | null; receipt: Receipt; facts: ReviewFact[]; missing_fact_ids: number[]; source: ReviewSource };
export interface FactReviewResponse {
  schema_version: 1;
  limits: { max_jobs: number; max_facts: number };
  jobs: FactReviewJob[];
}

const iso = (v: unknown): string | null => v == null ? null : new Date(v as string).toISOString();
const count = (v: unknown): number | null => Number.isSafeInteger(v) && (v as number) >= 0 ? v as number : null;
const str = (v: unknown): string | null => typeof v === 'string' ? v : null;

/** Only the handler's structured result is trusted; one malformed id voids the whole receipt. */
function readReceipt(result: unknown): Receipt {
  if (!result || typeof result !== 'object' || Array.isArray(result)) return { status: 'invalid' };
  const r = result as Record<string, unknown>;
  if (Array.isArray(r.fact_ids)) {
    if (!r.fact_ids.every(id => Number.isSafeInteger(id) && (id as number) > 0)) return { status: 'invalid' };
    return { status: 'facts', inserted: count(r.inserted), duplicate: count(r.duplicate), superseded: count(r.superseded),
      fact_ids: [...new Set(r.fact_ids as number[])] };
  }
  if (typeof r.skipped === 'string') return { status: 'skipped', reason: /^[\w:.-]{1,80}$/.test(r.skipped) ? r.skipped : 'unspecified' };
  return { status: 'invalid' };
}

async function readFacts(engine: BrainEngine, sourceId: string, ids: number[]): Promise<ReviewFact[]> {
  if (!ids.length) return [];
  // Whitelisted projection, scoped to the job's source: a cross-source receipt id matches nothing.
  const rows = await engine.executeRaw<Record<string, unknown>>(`SELECT id::text AS id, fact, kind, entity_slug, visibility, notability,
      source, context, valid_from, valid_until, expired_at, superseded_by::text AS superseded_by, confidence, created_at
    FROM facts WHERE source_id = $1 AND id = ANY($2::bigint[]) ORDER BY id`, [sourceId, ids]);
  return rows.map(r => ({
    id: Number(r.id), fact: String(r.fact), kind: String(r.kind), entity_slug: str(r.entity_slug), visibility: String(r.visibility),
    notability: String(r.notability), source: String(r.source), context: str(r.context), valid_from: iso(r.valid_from),
    valid_until: iso(r.valid_until), expired_at: iso(r.expired_at), superseded_by: r.superseded_by == null ? null : Number(r.superseded_by),
    confidence: Number(r.confidence), created_at: iso(r.created_at),
    state: r.superseded_by != null ? 'superseded' : r.expired_at != null ? 'expired' : 'active',
  }));
}

function text(title: string, type: string, body: string): Pick<SourceSnapshot, 'title' | 'type' | 'body' | 'body_truncated'> {
  return { title, type, body: body.slice(0, FACT_REVIEW_MAX_BODY_CHARS), body_truncated: body.length > FACT_REVIEW_MAX_BODY_CHARS };
}

async function readSource(engine: BrainEngine, job: MinionJob, sourceId: string): Promise<ReviewSource> {
  const data = job.data ?? {};
  const slug = str(data.slug);
  const pageId = Number.isSafeInteger(data.page_id) ? data.page_id as number : null;
  const revision = typeof data.revision === 'string' && UUID.test(data.revision) ? data.revision.toLowerCase() : null;
  const rawRequestId = data.persistence_request_id;
  const source: Omit<ReviewSource, 'snapshot'> = { source_id: sourceId, slug, page_id: pageId, revision,
    persistence_request_id: typeof rawRequestId === 'string' && UUID.test(rawRequestId) ? rawRequestId : null, request: 'absent' };
  if (!slug) return { ...source, snapshot: { status: 'unavailable' } };

  // The page text at the job's revision is exactly what the extraction read.
  const current = await engine.readPageSnapshot(slug, { sourceId });
  let atRevision: { title: string; type: string; body: string; recorded_at: string | null; current: boolean } | null = null;
  if (revision && current && current.page.id === pageId && current.revision.toLowerCase() === revision) {
    atRevision = { title: current.page.title, type: current.page.type, body: current.page.compiled_truth, recorded_at: iso(current.page.updated_at), current: true };
  } else if (revision && pageId !== null) {
    const version = (await engine.getVersions(slug, { sourceId }))
      .find(v => v.page_id === pageId && String(v.knowledge_revision ?? '').toLowerCase() === revision);
    if (version) atRevision = { title: version.title ?? '', type: version.type ?? '', body: version.compiled_truth, recorded_at: iso(version.snapshot_at), current: false };
  }

  let input: { provenance: Omit<RequestInputProvenance, 'comparison'>; title: string; type: string; body: string } | null = null;
  if (rawRequestId !== undefined) {
    if (!source.persistence_request_id) source.request = 'invalid_id';
    else {
      const row = await getWriteRequestById(engine, source.persistence_request_id);
      if (!row) source.request = 'not_found';
      else if (row.state !== 'committed' || row.source_id !== sourceId || row.slug !== slug || (row.page_id != null && row.page_id !== pageId)) source.request = 'mismatch';
      else if (!row.intent) source.request = 'compacted';
      else if (typeof row.intent.content !== 'string') source.request = 'no_content';
      else {
        source.request = 'used';
        const parsed = parseMarkdown(row.intent.content, row.slug);
        input = { title: parsed.title, type: parsed.type, body: parsed.compiled_truth, provenance: { operation: row.operation,
          source_kind: str(row.intent.source_kind), source_uri: str(row.intent.source_uri), ingested_via: str(row.intent.ingested_via),
          recorded_at: iso(row.created_at) } };
      }
    }
  }
  // Revision-fenced text is primary whenever it exists; the request input never replaces it.
  if (atRevision) {
    const request_input: RequestInputProvenance | undefined = !input ? undefined : input.body === atRevision.body
      ? { comparison: 'identical', ...input.provenance }
      : { comparison: 'differs', ...input.provenance, ...text(input.title, input.type, input.body) };
    return { ...source, snapshot: { status: atRevision.current ? 'current_revision_match' : 'revision_version',
      ...text(atRevision.title, atRevision.type, atRevision.body), recorded_at: atRevision.recorded_at, ...(request_input ? { request_input } : {}) } };
  }
  if (input) {
    return { ...source, snapshot: { status: 'request_input_unverified', ...text(input.title, input.type, input.body),
      recorded_at: input.provenance.recorded_at, request_input: { comparison: 'not_compared', ...input.provenance } } };
  }
  if (!current) return { ...source, snapshot: { status: 'unavailable' } };
  // Current-page fallback: never extraction-time truth unless a recorded marker proves it.
  let status: SnapshotStatus = revision ? 'current_changed' : 'current_unverified';
  const prefix = `facts-absorb:${sourceId}:${slug}:`;
  const submitted = !revision && job.idempotency_key?.startsWith(prefix) ? job.idempotency_key.slice(prefix.length) : '';
  if (/^[0-9a-f]{16}$/.test(submitted)) {
    const hash = createHash('sha256').update(current.page.compiled_truth).digest('hex').slice(0, 16);
    status = hash === submitted ? 'current_hash_match' : 'current_changed';
  }
  return { ...source, snapshot: { status, ...text(current.page.title, current.page.type, current.page.compiled_truth), recorded_at: iso(current.page.updated_at) } };
}

export async function readFactReview(engine: BrainEngine, jobIds: number[]): Promise<FactReviewResponse> {
  const queue = new MinionQueue(engine);
  const selected: Array<{ done: FactReviewJob } | { job: MinionJob; receipt: Receipt; sourceId: string }> = [];
  for (const id of jobIds) {
    const job = await queue.getJob(id);
    if (!job) selected.push({ done: { job_id: id, status: 'not_found' } });
    else if (job.name !== 'facts-absorb' || job.queue !== 'default') selected.push({ done: { job_id: id, status: 'not_facts_absorb' } });
    else if (job.status !== 'completed') selected.push({ done: { job_id: id, status: 'not_completed', job_status: job.status } });
    else selected.push({ job, receipt: readReceipt(job.result), sourceId: typeof job.data?.sourceId === 'string' ? job.data.sourceId : 'default' });
  }
  const total = selected.reduce((n, s) => n + ('job' in s && s.receipt.status === 'facts' ? s.receipt.fact_ids.length : 0), 0);
  if (total > FACT_REVIEW_MAX_FACTS) throw new FactReviewLimitError();

  const jobs: FactReviewJob[] = [];
  for (const s of selected) {
    if ('done' in s) { jobs.push(s.done); continue; }
    const ids = s.receipt.status === 'facts' ? s.receipt.fact_ids : [];
    const facts = await readFacts(engine, s.sourceId, ids);
    const found = new Set(facts.map(f => f.id));
    jobs.push({ job_id: s.job.id, status: 'completed', finished_at: iso(s.job.finished_at), receipt: s.receipt, facts,
      missing_fact_ids: ids.filter(id => !found.has(id)), source: await readSource(engine, s.job, s.sourceId) });
  }
  return { schema_version: 1, limits: { max_jobs: FACT_REVIEW_MAX_JOBS, max_facts: FACT_REVIEW_MAX_FACTS }, jobs };
}

/** GET /admin/api/facts-absorb/review — mounted behind requireAdmin. Errors are opaque and nothing is logged. */
export async function serveFactReview(engine: BrainEngine, req: Request, res: Response): Promise<void> {
  res.set('Cache-Control', 'private, no-store');
  let parsed: FactReviewQuery;
  try { parsed = parseFactReviewQuery(req.query); } catch { parsed = { ok: false, error: 'invalid_job_ids' }; }
  if (!parsed.ok) { res.status(400).json({ error: parsed.error }); return; }
  try {
    res.json(await readFactReview(engine, parsed.jobIds));
  } catch (e) {
    if (e instanceof FactReviewLimitError) res.status(400).json({ error: e.code, max_facts: FACT_REVIEW_MAX_FACTS });
    else res.status(503).json({ error: 'service_unavailable' });
  }
}
