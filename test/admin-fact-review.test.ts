/**
 * MEM-01 owner fact review: completed facts-absorb receipts → source-scoped
 * facts → the source text the extraction read. Fixtures run the real
 * admit → publish → dispatch path, so job lineage, page revisions and request
 * intents have their production shape. All content is synthetic. PostgreSQL
 * runs only against a fresh isolated database when DATABASE_URL is explicitly
 * provided (see test/helpers/test-backends.ts).
 */
import { afterAll, beforeAll, describe, expect, spyOn, test } from 'bun:test';
import { createHash, randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { BrainEngine } from '../src/core/engine.ts';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { MinionQueue } from '../src/core/minions/queue.ts';
import type { OperationContext } from '../src/core/ops/contract.ts';
import { submissionAuthority } from '../src/core/persistence/authority.ts';
import { admitWrite, claimNextWrite } from '../src/core/persistence/journal.ts';
import { localHostId, registerLocalWriter } from '../src/core/persistence/identity.ts';
import { preparePageMutation } from '../src/core/persistence/page-prepare.ts';
import { publishMutation } from '../src/core/persistence/coordinator.ts';
import { claimPersistenceEffect } from '../src/core/persistence/effect-journal.ts';
import { dispatchFactsBackstopEffect } from '../src/core/persistence/effect-facts.ts';
import type { WriteRequest } from '../src/core/persistence/model.ts';
import {
  FACT_REVIEW_MAX_FACTS, FACT_REVIEW_MAX_JOBS, parseFactReviewQuery, readFactReview, serveFactReview,
} from '../src/commands/admin-fact-review.ts';
import { withEnv } from './helpers/with-env.ts';
import { isolatedPersistencePostgres } from './helpers/persistence-postgres.ts';
import { testBackends } from './helpers/test-backends.ts';

const backends = testBackends();
const engines = new Map<string, BrainEngine>();
let closePostgres: (() => Promise<void>) | undefined;
const home = mkdtempSync(join(tmpdir(), 'gbrain-fact-review-'));

beforeAll(async () => {
  if (backends.includes('pglite')) {
    const lite = new PGLiteEngine();
    await lite.connect({});
    await lite.initSchema();
    engines.set('pglite', lite);
  }
  if (backends.includes('postgres')) {
    const pg = await isolatedPersistencePostgres(process.env.DATABASE_URL!);
    engines.set('postgres', pg.engine);
    closePostgres = pg.close;
  }
  for (const engine of engines.values()) {
    await withEnv({ GBRAIN_HOME: home }, async () => {
      await registerLocalWriter(engine, 'cli');
      await engine.executeRaw(`INSERT INTO sources (id, name) VALUES ('synthetic-other', 'Synthetic other') ON CONFLICT (id) DO NOTHING`);
    });
  }
}, 120_000);

afterAll(async () => {
  for (const engine of engines.values()) if (engine.kind === 'pglite') await engine.disconnect();
  await closePostgres?.();
  rmSync(home, { recursive: true, force: true });
}, 30_000);

const FILLER = 'A substantive synthetic record of the example project and its current status. '.repeat(4);
const doc = (title: string, marker: string) => `---\ntype: note\ntitle: ${title}\n---\n\n${marker} ${FILLER}`;
const config = { engine: 'pglite' as const, embedding_disabled: true };

/** Real persistence write + facts-backstop dispatch; returns the committed request and its facts-absorb job id. */
async function publish(engine: BrainEngine, slug: string, content: string): Promise<{ request: WriteRequest; jobId: number }> {
  return withEnv({ GBRAIN_HOME: home }, async () => {
    const [source] = await engine.executeRaw<{ incarnation: string }>("SELECT incarnation FROM sources WHERE id='default'");
    const ctx: OperationContext = { engine, config, dryRun: false, remote: false, sourceId: 'default', logger: { info() {}, warn() {}, error() {} } } as OperationContext;
    const current = await engine.readPageSnapshot(slug, { sourceId: 'default' });
    const intent: Record<string, unknown> = { content, ...(current ? { expected_revision: current.revision } : {}) };
    const authority = await submissionAuthority(ctx, 'put_page', 'default', source.incarnation, slug);
    await admitWrite(engine, { principal: authority.principal, authority, operation: 'put_page', sourceId: 'default',
      sourceIncarnation: source.incarnation, slug, pageId: current?.page.id ?? null, requestId: randomUUID(), callerIntent: intent, intent });
    const row = (await claimNextWrite(engine, localHostId()))!;
    const request = await publishMutation(engine, row, await preparePageMutation(engine, row, config));
    expect(request.state).toBe('committed');
    await engine.executeRaw("UPDATE persistence_effects SET next_attempt_at=now()+interval '1 hour' WHERE kind<>'facts-backstop' AND state<>'committed'");
    const effect = (await claimPersistenceEffect(engine, localHostId()))!;
    expect(effect.request_id).toBe(request.id);
    await dispatchFactsBackstopEffect(engine, effect, localHostId());
    const [job] = await engine.executeRaw<{ id: number }>(
      "SELECT id FROM minion_jobs WHERE name='facts-absorb' AND data->>'persistence_request_id'=$1", [request.id]);
    return { request, jobId: Number(job.id) };
  });
}

async function complete(engine: BrainEngine, jobId: number, result: unknown): Promise<void> {
  await engine.executeRaw(`UPDATE minion_jobs SET status='completed', result=$2::text::jsonb, finished_at=now() WHERE id=$1`,
    [jobId, JSON.stringify(result)]);
}

interface FactSeed { source?: string; entity?: string; kind?: string; visibility?: string; expired?: boolean; supersededBy?: number | null }
async function fact(engine: BrainEngine, text: string, seed: FactSeed = {}): Promise<number> {
  const [row] = await engine.executeRaw<{ id: string }>(`INSERT INTO facts
      (source_id, entity_slug, fact, kind, visibility, notability, context, source, source_session, confidence, valid_until, expired_at, superseded_by)
    VALUES ($1, $2, $3, $4, $5, 'high', 'synthetic context', 'mcp:put_page', 'session-marker-4f1c', 0.75,
      now() + interval '30 days', CASE WHEN $6 THEN now() ELSE NULL END, $7)
    RETURNING id::text AS id`,
  [seed.source ?? 'default', seed.entity ?? 'people/alice-example', text, seed.kind ?? 'fact', seed.visibility ?? 'private', seed.expired === true, seed.supersededBy ?? null]);
  return Number(row.id);
}

function inline(factIds: unknown[], extra: Record<string, unknown> = {}) {
  return { mode: 'inline', inserted: factIds.length, duplicate: 0, superseded: 0, fact_ids: factIds, ...extra };
}

const FACT_KEYS = ['confidence', 'context', 'created_at', 'entity_slug', 'expired_at', 'fact', 'id', 'kind', 'notability',
  'source', 'state', 'superseded_by', 'valid_from', 'valid_until', 'visibility'];

describe('parseFactReviewQuery', () => {
  test('accepts exactly one comma-separated string of unique positive job ids, in caller order', () => {
    expect(parseFactReviewQuery({ job_ids: '3,1,2' })).toEqual({ ok: true, jobIds: [3, 1, 2] });
    expect(parseFactReviewQuery({ job_ids: '2147483647' })).toEqual({ ok: true, jobIds: [2147483647] });
    const eight = Array.from({ length: FACT_REVIEW_MAX_JOBS }, (_, i) => i + 1);
    expect(parseFactReviewQuery({ job_ids: eight.join(',') })).toEqual({ ok: true, jobIds: eight });
  });

  const rejected: Array<[string, unknown]> = [
    ['missing parameter', {}],
    ['empty string', { job_ids: '' }],
    ['repeated parameter (array)', { job_ids: ['1', '2'] }],
    ['nested object', { job_ids: { a: '1' } }],
    ['nested array element', { job_ids: [['1']] }],
    ['number instead of string', { job_ids: 1 }],
    ['extra parameter', { job_ids: '1', slug: 'x' }],
    ['non-object query', 'job_ids=1'],
    ['null query', null],
    ['empty segment', { job_ids: '1,,2' }],
    ['trailing comma', { job_ids: '1,' }],
    ['zero', { job_ids: '0' }],
    ['negative', { job_ids: '-1' }],
    ['leading zero', { job_ids: '01' }],
    ['decimal', { job_ids: '1.5' }],
    ['exponent', { job_ids: '1e3' }],
    ['whitespace', { job_ids: ' 1' }],
    ['SQL-like', { job_ids: '1;DROP TABLE facts' }],
    ['SQL-like boolean', { job_ids: '1 OR 1=1' }],
    ['non-ASCII digits', { job_ids: '١' }],
    ['duplicate id', { job_ids: '4,4' }],
    ['beyond the job-id column', { job_ids: '2147483648' }],
    ['too many ids', { job_ids: Array.from({ length: FACT_REVIEW_MAX_JOBS + 1 }, (_, i) => i + 1).join(',') }],
    ['oversized string', { job_ids: '1'.repeat(500) }],
  ];
  for (const [label, query] of rejected) {
    test(`rejects ${label} with one opaque error`, () => {
      expect(parseFactReviewQuery(query)).toEqual({ ok: false, error: 'invalid_job_ids' });
    });
  }
});

for (const backend of backends) {
  describe(`readFactReview (${backend})`, () => {
    const db = () => engines.get(backend)!;

    test('returns receipt facts with whitelisted fields and the exact persistence-request source', async () => {
      const engine = db();
      const { request, jobId } = await publish(engine, 'notes/review-exact', doc('Synthetic Review Notes', 'exact-body-marker-a1'));
      const active = await fact(engine, 'Alice-example prefers synthetic tea.');
      const expired = await fact(engine, 'Alice-example lived in example-town.', { expired: true, kind: 'event' });
      const superseded = await fact(engine, 'Alice-example prefers synthetic coffee.', { supersededBy: active, kind: 'preference' });
      await complete(engine, jobId, inline([active, expired, superseded], {
        write_requests: [{ request_id: 'receipt-extra-marker-77' }], entity_slugs: ['people/alice-example'],
      }));

      const review = await readFactReview(engine, [jobId]);
      expect(review.schema_version).toBe(1);
      expect(review.limits).toEqual({ max_jobs: FACT_REVIEW_MAX_JOBS, max_facts: FACT_REVIEW_MAX_FACTS });
      const job = review.jobs[0] as any;
      expect(job).toMatchObject({ job_id: jobId, status: 'completed', missing_fact_ids: [] });
      expect(job.receipt).toEqual({ status: 'facts', inserted: 3, duplicate: 0, superseded: 0, fact_ids: [active, expired, superseded] });
      expect(job.facts.map((f: any) => [f.id, f.state])).toEqual([[active, 'active'], [expired, 'expired'], [superseded, 'superseded']]);
      for (const f of job.facts) expect(Object.keys(f).sort()).toEqual(FACT_KEYS);
      expect(job.facts[0]).toMatchObject({ fact: 'Alice-example prefers synthetic tea.', kind: 'fact', entity_slug: 'people/alice-example',
        visibility: 'private', notability: 'high', source: 'mcp:put_page', context: 'synthetic context', confidence: 0.75,
        superseded_by: null, expired_at: null });
      expect(typeof job.facts[0].valid_from).toBe('string');
      expect(typeof job.facts[0].valid_until).toBe('string');
      expect(job.facts[2].superseded_by).toBe(active);

      const [page] = await engine.executeRaw<{ id: number; knowledge_revision: string }>(
        "SELECT id, knowledge_revision::text AS knowledge_revision FROM pages WHERE slug='notes/review-exact' AND source_id='default'");
      expect(job.source).toMatchObject({ source_id: 'default', slug: 'notes/review-exact', page_id: Number(page.id),
        revision: page.knowledge_revision, persistence_request_id: request.id, request: 'used' });
      expect(job.source.snapshot).toMatchObject({ status: 'current_revision_match', title: 'Synthetic Review Notes', type: 'note', body_truncated: false,
        request_input: { comparison: 'identical', operation: 'put_page' } });
      expect(job.source.snapshot.body).toContain('exact-body-marker-a1');
      expect(job.source.snapshot.request_input).not.toHaveProperty('body'); // identical input is not repeated

      const json = JSON.stringify(review);
      for (const secret of ['session-marker-4f1c', 'receipt-extra-marker-77', request.principal_id, request.digest, '"authority"', '"recovery"',
        'embedding', 'facts-absorb:write:', 'expected_revision']) expect(json).not.toContain(secret);
    });

    test('reads receipt ids only inside the job source; foreign and absent ids are reported, never returned', async () => {
      const engine = db();
      const { jobId } = await publish(engine, 'notes/review-scope', doc('Scope Notes', 'scope-body-marker-b2'));
      const own = await fact(engine, 'Charlie-example owns a synthetic bicycle.');
      const foreign = await fact(engine, 'foreign-source-fact-marker-9d', { source: 'synthetic-other' });
      const unlisted = await fact(engine, 'unlisted-same-source-fact-marker-3e');
      await complete(engine, jobId, inline([own, foreign, 2_000_000_001]));

      const job = (await readFactReview(engine, [jobId])).jobs[0] as any;
      expect(job.facts.map((f: any) => f.id)).toEqual([own]);
      expect(job.missing_fact_ids).toEqual([foreign, 2_000_000_001]);
      const json = JSON.stringify(job);
      expect(json).not.toContain('foreign-source-fact-marker-9d');
      expect(json).not.toContain('unlisted-same-source-fact-marker-3e');
      expect(unlisted).toBeGreaterThan(0);
    });

    test('corrupt and skipped receipts read no facts; an oversized total is refused before any fact read', async () => {
      const engine = db();
      const { jobId: corrupt } = await publish(engine, 'notes/review-corrupt', doc('Corrupt Notes', 'corrupt-marker-c3'));
      const leaked = await fact(engine, 'corrupt-receipt-must-not-read-marker');
      await complete(engine, corrupt, inline([String(leaked), leaked]));
      const { jobId: skipped } = await publish(engine, 'notes/review-skipped', doc('Skipped Notes', 'skipped-marker-d4'));
      await complete(engine, skipped, { skipped: 'superseded', slug: 'notes/review-skipped', sourceId: 'default' });
      const { jobId: empty } = await publish(engine, 'notes/review-null', doc('Null Notes', 'null-marker-e5'));
      await complete(engine, empty, null);

      const review = await readFactReview(engine, [corrupt, skipped, empty]);
      const [a, b, c] = review.jobs as any[];
      expect(a).toMatchObject({ status: 'completed', receipt: { status: 'invalid' }, facts: [], missing_fact_ids: [] });
      expect(b).toMatchObject({ status: 'completed', receipt: { status: 'skipped', reason: 'superseded' }, facts: [] });
      expect(c).toMatchObject({ status: 'completed', receipt: { status: 'invalid' }, facts: [] });
      expect(JSON.stringify(review)).not.toContain('corrupt-receipt-must-not-read-marker');

      const { jobId: big1 } = await publish(engine, 'notes/review-big-1', doc('Big One', 'big-marker-1'));
      const { jobId: big2 } = await publish(engine, 'notes/review-big-2', doc('Big Two', 'big-marker-2'));
      const ids = (from: number) => Array.from({ length: 101 }, (_, i) => from + i);
      await complete(engine, big1, inline(ids(1_000_000)));
      await complete(engine, big2, inline(ids(1_100_000)));
      await expect(readFactReview(engine, [big1, big2])).rejects.toMatchObject({ code: 'too_many_facts' });
      expect((await readFactReview(engine, [big1])).jobs[0]).toMatchObject({ receipt: { status: 'facts' } });
    });

    test('unknown, unfinished, foreign-name and other-queue jobs get safe per-job statuses without payloads', async () => {
      const engine = db();
      const queue = new MinionQueue(engine);
      const waiting = await queue.add('facts-absorb', { slug: 'notes/waiting', note: 'waiting-payload-marker-f6' }, { queue: 'default' });
      const other = await queue.add('synthetic-other-job', { note: 'other-name-payload-marker-a7' }, { queue: 'default' });
      await complete(engine, other.id, inline([1], { note: 'other-name-result-marker-b8' }));
      const offQueue = await queue.add('facts-absorb', { slug: 'notes/off-queue', note: 'off-queue-payload-marker-c9' }, { queue: 'synthetic-queue' });
      await complete(engine, offQueue.id, inline([1]));

      const review = await readFactReview(engine, [2_147_000_000, waiting.id, other.id, offQueue.id]);
      expect(review.jobs).toEqual([
        { job_id: 2_147_000_000, status: 'not_found' },
        { job_id: waiting.id, status: 'not_completed', job_status: 'waiting' },
        { job_id: other.id, status: 'not_facts_absorb' },
        { job_id: offQueue.id, status: 'not_facts_absorb' },
      ]);
    });

    test('without the request input the exact revision is used: current page, then the recorded version, else a changed marker', async () => {
      const engine = db();
      const { request, jobId } = await publish(engine, 'notes/review-revision', doc('Revision Notes', 'original-body-marker-d0'));
      await complete(engine, jobId, inline([await fact(engine, 'Revision fact.')]));
      // Receipt compaction drops the normalized intent (compactWriteReceipts).
      await engine.executeRaw('UPDATE persistence_requests SET intent=NULL, compacted=true WHERE id=$1::uuid', [request.id]);

      let source = ((await readFactReview(engine, [jobId])).jobs[0] as any).source;
      expect(source.request).toBe('compacted');
      expect(source.snapshot).toMatchObject({ status: 'current_revision_match', title: 'Revision Notes' });
      expect(source.snapshot.body).toContain('original-body-marker-d0');

      await publish(engine, 'notes/review-revision', doc('Revision Notes Edited', 'edited-body-marker-e1'));
      source = ((await readFactReview(engine, [jobId])).jobs[0] as any).source;
      expect(source.snapshot).toMatchObject({ status: 'revision_version', title: 'Revision Notes' });
      expect(source.snapshot.body).toContain('original-body-marker-d0');
      expect(source.snapshot.body).not.toContain('edited-body-marker-e1');

      // A newer recorded version (of the edited revision) must not stand in for the extraction revision.
      await publish(engine, 'notes/review-revision', doc('Revision Notes Third', 'third-body-marker-e2'));
      source = ((await readFactReview(engine, [jobId])).jobs[0] as any).source;
      expect(source.snapshot).toMatchObject({ status: 'revision_version', title: 'Revision Notes' });
      expect(source.snapshot.body).toContain('original-body-marker-d0');

      await engine.executeRaw(`DELETE FROM page_versions WHERE page_id=$1`, [source.page_id]);
      source = ((await readFactReview(engine, [jobId])).jobs[0] as any).source;
      expect(source.snapshot).toMatchObject({ status: 'current_changed', title: 'Revision Notes Third' });
      expect(source.snapshot.body).toContain('third-body-marker-e2');
    });

    test('revision-fenced page text is the primary source; divergent request input is provenance only, and unverified without it', async () => {
      const engine = db();
      const { jobId } = await publish(engine, 'notes/review-compare', doc('Compare Notes', 'compare-marker-f2'));
      await complete(engine, jobId, inline([]));
      const snapshot = (await engine.readPageSnapshot('notes/review-compare', { sourceId: 'default' }))!;
      // A server-side normalization changed the published text but kept the revision.
      await engine.transaction(async tx => {
        await tx.executeRaw("SELECT set_config('gbrain.materializing_revision', $1, true)", [snapshot.revision]);
        await tx.executeRaw(`UPDATE pages SET compiled_truth = compiled_truth || ' normalized-tail' WHERE id=$1`, [snapshot.page.id]);
      });
      expect((await engine.readPageSnapshot('notes/review-compare', { sourceId: 'default' }))!.revision).toBe(snapshot.revision);
      // The handler extracts the revision-fenced page.compiled_truth, so that text (with the tail) is primary.
      const source = ((await readFactReview(engine, [jobId])).jobs[0] as any).source;
      expect(source.request).toBe('used');
      expect(source.snapshot).toMatchObject({ status: 'current_revision_match', title: 'Compare Notes',
        request_input: { comparison: 'differs', operation: 'put_page', title: 'Compare Notes' } });
      expect(source.snapshot.body).toContain('normalized-tail');
      expect(source.snapshot.request_input.body).toContain('compare-marker-f2');
      expect(source.snapshot.request_input.body).not.toContain('normalized-tail');

      // A later write moves the page on; the extraction-revision text survives as a recorded version and stays primary.
      await publish(engine, 'notes/review-compare', doc('Compare Notes Edited', 'compare-edit-marker-f3'));
      const later = ((await readFactReview(engine, [jobId])).jobs[0] as any).source;
      expect(later.snapshot).toMatchObject({ status: 'revision_version', title: 'Compare Notes', request_input: { comparison: 'differs' } });
      expect(later.snapshot.body).toContain('normalized-tail');
      expect(later.snapshot.body).not.toContain('compare-edit-marker-f3');

      // Without any revision-fenced text, the request input is shown only as unverified input — never as the current page.
      await engine.executeRaw(`DELETE FROM page_versions WHERE page_id=$1`, [snapshot.page.id]);
      const unretained = ((await readFactReview(engine, [jobId])).jobs[0] as any).source;
      expect(unretained.snapshot).toMatchObject({ status: 'request_input_unverified', title: 'Compare Notes',
        request_input: { comparison: 'not_compared', operation: 'put_page' } });
      expect(unretained.snapshot.body).toContain('compare-marker-f2');
      expect(unretained.snapshot.body).not.toContain('normalized-tail');
      expect(unretained.snapshot.body).not.toContain('compare-edit-marker-f3');
      expect(unretained.snapshot.request_input).not.toHaveProperty('body');
    });

    test('a request that does not match the job lineage is never used as its source', async () => {
      const engine = db();
      const { request: otherRequest } = await publish(engine, 'notes/review-lineage-other', doc('Other Lineage', 'other-lineage-marker-a3'));
      const { jobId } = await publish(engine, 'notes/review-lineage', doc('Lineage Notes', 'lineage-marker-b4'));
      await complete(engine, jobId, inline([]));
      await engine.executeRaw(`UPDATE minion_jobs SET data = jsonb_set(data, '{persistence_request_id}', to_jsonb($2::text)) WHERE id=$1`, [jobId, otherRequest.id]);
      let job = (await readFactReview(engine, [jobId])).jobs[0] as any;
      expect(job.source.request).toBe('mismatch');
      expect(job.source.snapshot.status).toBe('current_revision_match');
      expect(JSON.stringify(job)).not.toContain('other-lineage-marker-a3');

      await engine.executeRaw(`UPDATE minion_jobs SET data = jsonb_set(data, '{persistence_request_id}', '"not-a-uuid"') WHERE id=$1`, [jobId]);
      job = (await readFactReview(engine, [jobId])).jobs[0] as any;
      expect(job.source.request).toBe('invalid_id');
      await engine.executeRaw(`UPDATE minion_jobs SET data = jsonb_set(data, '{persistence_request_id}', to_jsonb($2::text)) WHERE id=$1`, [jobId, randomUUID()]);
      expect(((await readFactReview(engine, [jobId])).jobs[0] as any).source.request).toBe('not_found');
    });

    test('legacy jobs without a revision: submitted-hash match, changed, unverified and missing pages', async () => {
      const engine = db();
      const slug = 'notes/review-legacy';
      const body = `legacy-body-marker-c5 ${FILLER}`;
      await engine.putPage(slug, { type: 'note', title: 'Legacy Notes', compiled_truth: body }, { sourceId: 'default' } as never);
      const hash = createHash('sha256').update(body).digest('hex').slice(0, 16);
      const queue = new MinionQueue(engine);
      const hashed = await queue.add('facts-absorb', { slug, sourceId: 'default' }, { queue: 'default', idempotency_key: `facts-absorb:default:${slug}:${hash}` });
      await complete(engine, hashed.id, inline([]));
      const bare = await queue.add('facts-absorb', { slug, sourceId: 'default' }, { queue: 'default' });
      await complete(engine, bare.id, inline([]));
      const gone = await queue.add('facts-absorb', { slug: 'notes/review-never-written', sourceId: 'default' }, { queue: 'default' });
      await complete(engine, gone.id, inline([]));

      let [h, b, g] = (await readFactReview(engine, [hashed.id, bare.id, gone.id])).jobs as any[];
      expect(h.source).toMatchObject({ request: 'absent', revision: null, snapshot: { status: 'current_hash_match', title: 'Legacy Notes' } });
      expect(b.source.snapshot.status).toBe('current_unverified');
      expect(g.source.snapshot).toEqual({ status: 'unavailable' });

      await engine.putPage(slug, { type: 'note', title: 'Legacy Notes', compiled_truth: `changed-legacy-marker ${FILLER}` }, { sourceId: 'default' } as never);
      [h] = (await readFactReview(engine, [hashed.id])).jobs as any[];
      expect(h.source.snapshot.status).toBe('current_changed');
    });
  });
}

describe('serveFactReview', () => {
  function capture() {
    const out: { status: number; headers: Record<string, string>; body: unknown } = { status: 200, headers: {}, body: undefined };
    const res = {
      set(name: string, value: string) { out.headers[name.toLowerCase()] = value; return res; },
      status(code: number) { out.status = code; return res; },
      json(body: unknown) { out.body = body; return res; },
    };
    return { out, res: res as never };
  }

  test('sets private no-store on every outcome, refuses malformed input opaquely, and never logs private text', async () => {
    const engine = engines.values().next().value!;
    const { jobId } = await publish(engine, 'notes/review-handler', doc('Handler Notes', 'handler-body-marker-d6'));
    await complete(engine, jobId, inline([await fact(engine, 'handler-fact-marker-e7')]));
    const logs = ['log', 'info', 'warn', 'error', 'debug'].map(method => spyOn(console, method as 'log').mockImplementation(() => {}));
    try {
      const bad = capture();
      await serveFactReview(engine, { query: { job_ids: ['1', '2'] } } as never, bad.res);
      expect(bad.out).toEqual({ status: 400, headers: { 'cache-control': 'private, no-store' }, body: { error: 'invalid_job_ids' } });

      const ok = capture();
      await serveFactReview(engine, { query: { job_ids: String(jobId) } } as never, ok.res);
      expect(ok.out.status).toBe(200);
      expect(ok.out.headers['cache-control']).toBe('private, no-store');
      expect(JSON.stringify(ok.out.body)).toContain('handler-fact-marker-e7');

      const failing = new Proxy(engine, { get(target, key) {
        if (key === 'executeRaw') return async () => { throw new Error('db failure mentioning handler-fact-marker-e7'); };
        const value = Reflect.get(target, key); return typeof value === 'function' ? value.bind(target) : value;
      } });
      const down = capture();
      await serveFactReview(failing, { query: { job_ids: String(jobId) } } as never, down.res);
      expect(down.out).toEqual({ status: 503, headers: { 'cache-control': 'private, no-store' }, body: { error: 'service_unavailable' } });

      for (const spy of logs) expect(spy).not.toHaveBeenCalled();
    } finally { for (const spy of logs) spy.mockRestore(); }
  });
});
