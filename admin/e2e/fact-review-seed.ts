/**
 * Opt-in synthetic brain state for the Fact Review journey (GBRAIN_TEST_BROWSER_SEED=fact-review).
 * Every page, fact, marker and credential below is synthetic; the bearer tokens exist only in the
 * throwaway PGLite brain of one browser-fixture server.
 */
import type { BrainEngine } from '../../src/core/engine.ts';

export const FR = {
  exactJob: 9101, changedJob: 9102, waitingJob: 9103, otherJob: 9104, divergentJob: 9105, unverifiedJob: 9106,
  oauthAdminToken: 'gbrain_at_synthetic-fact-review-oauth-admin',
  legacyToken: 'gbrain_synthetic-fact-review-legacy-key',
  exactTitle: 'Synthetic Garden Notes',
  exactBody: 'exact-source-marker-7c21 The synthetic garden has three raised beds.',
  changedTitle: 'Synthetic Workshop Notes, edited later',
  changedBody: 'changed-source-marker-3b90 The example-workshop moved to a new shelf layout.',
  activeFact: 'Alice-example waters the synthetic garden on Sundays. <img src=x onerror="window.__factReviewXss=1">',
  supersededFact: 'Alice-example waters the synthetic garden on Saturdays.',
  expiredFact: 'Alice-example rented plot 7 at example-allotments.',
  changedFact: 'Charlie-example keeps synthetic tools in the example-workshop.',
  foreignFact: 'foreign-source-fact-marker-0d44',
  unlistedFact: 'unlisted-fact-marker-51ae',
  sessionMarker: 'session-marker-fr-6e0f',
  authorityMarker: 'authority-marker-fr-2c77',
  waitingPayload: 'waiting-payload-marker-fr-88aa',
  otherPayload: 'other-name-payload-marker-fr-19cd',
  divergentTitle: 'Synthetic Pantry Notes',
  divergentInput: 'divergent-input-marker-4e18 The synthetic pantry holds rice.',
  divergentPublishedTail: 'revision-only-tail-marker-a93f',
  unverifiedTitle: 'Synthetic Shed Notes',
  unverifiedInput: 'unverified-input-marker-6d02 The synthetic shed holds a ladder.',
  unverifiedCurrent: 'unverified-current-marker-b7c1 The synthetic shed was repainted.',
} as const;

export async function seedFactReview(engine: BrainEngine, publicUrl: string): Promise<{ foreignFactId: number }> {
  const { parseMarkdown } = await import('../../src/core/markdown.ts');
  const { hashToken } = await import('../../src/core/utils.ts');
  const sql = (query: string, params: unknown[] = []) => engine.executeRaw<Record<string, unknown>>(query, params);
  await sql(`INSERT INTO sources (id, name) VALUES ('synthetic-other', 'Synthetic other') ON CONFLICT (id) DO NOTHING`);
  const [{ incarnation }] = await sql(`SELECT incarnation::text AS incarnation FROM sources WHERE id='default'`);
  const fact = async (text: string, opts: { source?: string; kind?: string; expired?: boolean; supersededBy?: number } = {}) => Number((await sql(
    `INSERT INTO facts (source_id, entity_slug, fact, kind, visibility, notability, context, source, source_session, confidence, expired_at, superseded_by)
     VALUES ($1, 'people/alice-example', $2, $3, 'private', 'medium', 'synthetic garden notes', 'mcp:put_page', $4, 0.9,
       CASE WHEN $5 THEN now() ELSE NULL END, $6) RETURNING id::text AS id`,
    [opts.source ?? 'default', text, opts.kind ?? 'fact', FR.sessionMarker, opts.expired === true, opts.supersededBy ?? null]))[0].id);
  const job = (id: number, name: string, status: string, data: unknown, result: unknown) => sql(
    `INSERT INTO minion_jobs (id, submission_authority, name, queue, status, data, result, finished_at)
     VALUES ($1, '{"version":1,"kind":"application"}'::jsonb, $2, 'default', $3, $4::text::jsonb, $5::text::jsonb, $6::timestamptz)`,
    [id, name, status, JSON.stringify(data), result === undefined ? null : JSON.stringify(result), status === 'completed' ? new Date().toISOString() : null]);

  const request = async (slug: string, body: string) => String((await sql(`INSERT INTO persistence_requests (principal_kind, principal_id,
      request_id, operation, source_id, source_incarnation, slug, digest, intent, authority, intent_bytes, terminal_reservation, state, outcome, completed_at)
    VALUES ('local_cli', $1, gen_random_uuid(), 'put_page', 'default', $2::uuid, $3, 'synthetic-digest', $4::text::jsonb, $5::text::jsonb,
      0, 16384, 'committed', '{}'::jsonb, now()) RETURNING id::text AS id`,
  [FR.authorityMarker, incarnation, slug,
    JSON.stringify({ slug, content: body, source_kind: 'mcp:put_page', source_uri: null, ingested_via: 'mcp:put_page' }),
    JSON.stringify({ version: 1, principal: { kind: 'local_cli', id: FR.authorityMarker } })]))[0].id);

  // 9101: the persistence request input is retained and the page is still at the extraction revision.
  const exactSlug = 'notes/fact-review-garden';
  const content = `---\ntype: note\ntitle: ${FR.exactTitle}\n---\n\n${FR.exactBody}\n`;
  const parsed = parseMarkdown(content, exactSlug);
  await engine.putPage(exactSlug, { type: parsed.type, title: parsed.title, compiled_truth: parsed.compiled_truth }, { sourceId: 'default' });
  const exact = (await engine.readPageSnapshot(exactSlug, { sourceId: 'default' }))!;
  const requestId = await request(exactSlug, content);
  const active = await fact(FR.activeFact);
  const superseded = await fact(FR.supersededFact, { supersededBy: active });
  const expired = await fact(FR.expiredFact, { kind: 'event', expired: true });
  const foreignFactId = await fact(FR.foreignFact, { source: 'synthetic-other' });
  await fact(FR.unlistedFact);
  await job(FR.exactJob, 'facts-absorb', 'completed',
    { slug: exactSlug, sourceId: 'default', page_id: exact.page.id, revision: exact.revision, persistence_request_id: requestId, visibility: 'private' },
    { mode: 'inline', inserted: 3, duplicate: 1, superseded: 0, fact_ids: [active, superseded, expired, foreignFactId] });

  // 9102: the page changed after extraction and neither the request input nor a version is retained.
  const changedSlug = 'notes/fact-review-workshop';
  await engine.putPage(changedSlug, { type: 'note', title: FR.changedTitle, compiled_truth: FR.changedBody }, { sourceId: 'default' });
  const changed = (await engine.readPageSnapshot(changedSlug, { sourceId: 'default' }))!;
  await job(FR.changedJob, 'facts-absorb', 'completed',
    { slug: changedSlug, sourceId: 'default', page_id: changed.page.id, revision: '00000000-0000-4000-8000-000000000001' },
    { mode: 'inline', inserted: 1, duplicate: 0, superseded: 0, fact_ids: [await fact(FR.changedFact)] });

  // 9105: the page at the extraction revision differs from the request input (server-side normalization).
  const divergentSlug = 'notes/fact-review-pantry';
  const divergentContent = `---\ntype: note\ntitle: ${FR.divergentTitle}\n---\n\n${FR.divergentInput}\n`;
  const divergentParsed = parseMarkdown(divergentContent, divergentSlug);
  await engine.putPage(divergentSlug, { type: 'note', title: FR.divergentTitle,
    compiled_truth: `${divergentParsed.compiled_truth} ${FR.divergentPublishedTail}` }, { sourceId: 'default' });
  const divergent = (await engine.readPageSnapshot(divergentSlug, { sourceId: 'default' }))!;
  await job(FR.divergentJob, 'facts-absorb', 'completed',
    { slug: divergentSlug, sourceId: 'default', page_id: divergent.page.id, revision: divergent.revision,
      persistence_request_id: await request(divergentSlug, divergentContent) },
    { mode: 'inline', inserted: 0, duplicate: 0, superseded: 0, fact_ids: [] });

  // 9106: request input retained, but the page moved on and no version at the extraction revision exists.
  const unverifiedSlug = 'notes/fact-review-shed';
  await engine.putPage(unverifiedSlug, { type: 'note', title: FR.unverifiedTitle, compiled_truth: FR.unverifiedCurrent }, { sourceId: 'default' });
  const unverified = (await engine.readPageSnapshot(unverifiedSlug, { sourceId: 'default' }))!;
  await job(FR.unverifiedJob, 'facts-absorb', 'completed',
    { slug: unverifiedSlug, sourceId: 'default', page_id: unverified.page.id, revision: '00000000-0000-4000-8000-000000000002',
      persistence_request_id: await request(unverifiedSlug, `---\ntype: note\ntitle: ${FR.unverifiedTitle}\n---\n\n${FR.unverifiedInput}\n`) },
    { mode: 'inline', inserted: 0, duplicate: 0, superseded: 0, fact_ids: [] });

  await job(FR.waitingJob, 'facts-absorb', 'waiting', { slug: exactSlug, note: FR.waitingPayload }, undefined);
  await job(FR.otherJob, 'synthetic-other-job', 'completed', { note: FR.otherPayload }, { fact_ids: [active], note: FR.otherPayload });

  // Ordinary bearers that work on /mcp but must never reach owner review: an admin-scope OAuth token and a legacy full-access key.
  await sql(`INSERT INTO oauth_clients (client_id, client_name, grant_types, scope, token_endpoint_auth_method, client_id_issued_at, source_id, federated_read)
    VALUES ('synthetic-admin-client', 'Synthetic admin client', '{client_credentials}', 'read write admin', 'client_secret_post',
      extract(epoch from now())::bigint, 'default', '{default}')`);
  await sql(`INSERT INTO oauth_tokens (token_hash, token_type, client_id, scopes, expires_at, resource)
    VALUES ($1, 'access', 'synthetic-admin-client', '{read,write,admin}', extract(epoch from now())::bigint + 3600, $2)`,
  [hashToken(FR.oauthAdminToken), new URL('/mcp', publicUrl).toString()]);
  await sql(`INSERT INTO access_tokens (name, token_hash) VALUES ('synthetic-legacy-key', $1)`, [hashToken(FR.legacyToken)]);
  return { foreignFactId };
}
