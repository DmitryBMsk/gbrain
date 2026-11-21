/**
 * MEM-01 production candidate: an explicit extraction model for the ordinary
 * `gbrain jobs drain facts-absorb` consumer.
 *
 * `--model <provider:model>` makes the preview evaluate that model (chat
 * availability, pricing, operator-override conflict) and makes the REAL
 * builtin facts-absorb handler (backstop → extractor) request it — over both
 * the configured default and a queued payload model — without rewriting the
 * durable payload, the engine config or any other handler's defaults.
 * Without `--model`, per-job payload models and the configured default
 * behave exactly as before.
 *
 * PGLite in memory; every model call goes through the gateway test
 * transports (no provider, no network, synthetic keys only).
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { MinionQueue } from '../src/core/minions/queue.ts';
import {
  __setChatTransportForTests,
  __setEmbedTransportForTests,
  configureGateway,
  resetGateway,
  type ChatResult,
} from '../src/core/ai/gateway.ts';
import { canonicalLookup } from '../src/core/model-pricing.ts';
import {
  DrainUsageError,
  parseDrainArgs,
  previewDrain,
  runDrain,
  type DrainDeps,
  type DrainOptions,
} from '../src/commands/jobs-drain.ts';

const MODEL = 'google:gemini-3.8-flash';
const DEFAULT_MODEL = 'google:gemini-2.5-flash';
const PAYLOAD_MODEL = 'google:gemini-2.5-flash';
const LONG_BODY = 'synthetic drain model meeting note that is comfortably longer than eighty characters '.repeat(3);
const FAST: DrainDeps = { pollMs: 50, workerPollMs: 25, graceMs: 400 };

let engine: PGLiteEngine;
beforeAll(async () => {
  engine = new PGLiteEngine();
  await engine.connect({});
  await engine.initSchema();
}, 120_000);
afterAll(async () => { await engine.disconnect(); });

const tmpDirs: string[] = [];
afterEach(() => {
  __setChatTransportForTests(null);
  __setEmbedTransportForTests(null);
  resetGateway();
  for (const d of tmpDirs.splice(0)) rmSync(d, { recursive: true, force: true });
});

let requested: string[] = [];
function chatStub(): void {
  requested = [];
  __setChatTransportForTests(async (o): Promise<ChatResult> => {
    const model = (o as { model?: string }).model ?? '(unset)';
    requested.push(model);
    return {
      text: JSON.stringify({ facts: [{ fact: 'synthetic drain model fact', kind: 'event', entity: null, confidence: 1, notability: 'high' }] }),
      blocks: [], stopReason: 'end',
      usage: { input_tokens: 1000, output_tokens: 200, cache_read_tokens: 0, cache_creation_tokens: 0 },
      model, providerId: 'test',
    };
  });
}

async function fresh(): Promise<void> {
  await engine.executeRaw('TRUNCATE minion_jobs, facts, pages CASCADE');
  await engine.executeRaw(`DELETE FROM config WHERE key LIKE 'facts.%' OR key LIKE 'models.%' OR key = 'pricing.overrides'`);
  await engine.setConfig('facts.extraction_model', DEFAULT_MODEL);
  chatStub();
  __setEmbedTransportForTests((async ({ values }: { values: string[] }) =>
    ({ embeddings: values.map(() => Array.from({ length: 1536 }, () => 0.1)) })) as never);
}

async function factsJob(n: number, data: Record<string, unknown> = {}): Promise<{ id: number; slug: string }> {
  const slug = `meetings/drain-model-example-${n}`;
  await engine.putPage(slug, { type: 'meeting', title: `Drain model example ${n}`, compiled_truth: LONG_BODY }, { sourceId: 'default' } as never);
  const job = await new MinionQueue(engine).add('facts-absorb',
    { slug, sourceId: 'default', source: 'mcp:put_page', notabilityFilter: 'all', visibility: 'private', ...data },
    { queue: 'default', max_attempts: 5 });
  return { id: job.id, slug };
}

/** Durable identity of a queued job: payload + authority exactly as stored. */
async function jobIdentity(id: number): Promise<string> {
  const [row] = await engine.executeRaw<{ r: string }>(
    `SELECT json_build_object('data', data, 'submission_authority', submission_authority, 'name', name, 'queue', queue)::text AS r
       FROM minion_jobs WHERE id = $1`, [id]);
  return row!.r;
}
async function configRows(): Promise<string[]> {
  const rows = await engine.executeRaw<{ r: string }>(`SELECT key || '=' || coalesce(value, '') AS r FROM config ORDER BY key`);
  return rows.map((x) => x.r);
}
async function factRows(): Promise<Array<Record<string, unknown>>> {
  return engine.executeRaw<Record<string, unknown>>(
    `SELECT source_id, entity_slug, fact, kind, visibility, notability, context, source, source_session, confidence
       FROM facts ORDER BY id`);
}
async function allRows(): Promise<string[]> {
  const rows = await engine.executeRaw<{ r: string }>(`SELECT row_to_json(j)::text AS r FROM minion_jobs j ORDER BY id`);
  return rows.map((x) => x.r);
}

/**
 * Interpose on `pricing.overrides` reads only; `read(n)` answers the n-th
 * read. The stored config is never written through this seam, and the
 * engine's own getConfig is restored afterwards.
 */
async function withPricingReads<T>(read: (n: number) => Promise<string | null>, fn: () => Promise<T>): Promise<{ result: T; reads: number }> {
  const real = engine.getConfig.bind(engine);
  let reads = 0;
  engine.getConfig = (async (key: string) => (key === 'pricing.overrides' ? read(++reads) : real(key))) as typeof engine.getConfig;
  try {
    const result = await fn();
    return { result, reads };
  } finally {
    delete (engine as unknown as Record<string, unknown>).getConfig;
  }
}
const CONFLICT = JSON.stringify({ [MODEL]: { input: 0.0001, output: 0.0001 } });
const unreadable = async (): Promise<never> => { throw new Error('synthetic pricing read failure'); };

function opts(over: Partial<DrainOptions> = {}): DrainOptions & { execute: true } {
  return { name: 'facts-absorb', queue: 'default', maxJobs: 4, maxUsd: 1, maxRuntimeMs: 20_000, json: true, ...over, execute: true };
}
function auditPath(): string {
  const dir = mkdtempSync(join(tmpdir(), 'gbrain-mem01-drain-model-'));
  tmpDirs.push(dir);
  return join(dir, 'budget.jsonl');
}

describe('argument contract (engine-free)', () => {
  const base = ['drain', 'facts-absorb', '--max-jobs', '2', '--max-usd', '1'];

  test('--model takes one provider-qualified model id, in either flag spelling', () => {
    expect(parseDrainArgs([...base, '--model', MODEL]).model).toBe(MODEL);
    expect(parseDrainArgs([...base, `--model=${MODEL}`]).model).toBe(MODEL);
    expect(parseDrainArgs([...base, '--model', 'openrouter:google/gemini-3.8-flash']).model).toBe('openrouter:google/gemini-3.8-flash');
  });

  test('without --model the option is absent (configured and per-job models apply)', () => {
    expect(parseDrainArgs(base).model).toBeUndefined();
  });

  test('malformed, duplicated or valueless --model is rejected before any brain is touched', () => {
    for (const bad of [
      [...base, '--model'],
      [...base, '--model', '--yes'],
      [...base, '--model='],
      [...base, '--model', 'gemini-3.8-flash'],          // no provider
      [...base, '--model', 'google:'],
      [...base, '--model', ':gemini-3.8-flash'],
      [...base, '--model', 'google:gemini 3.8'],
      [...base, '--model', 'google:"gemini"'],
      [...base, '--model', MODEL, '--model', MODEL],
    ]) {
      expect(() => parseDrainArgs(bad)).toThrow(DrainUsageError);
    }
  });
});

describe('explicit model routes the real builtin handler', () => {
  test('--model 3.8 previews 3.8 and the real facts-absorb handler requests 3.8 over a 2.5 default and a 2.5 payload', async () => {
    await fresh();
    const { id, slug } = await factsJob(1, { model: PAYLOAD_MODEL });
    const identityBefore = await jobIdentity(id);
    const configBefore = await configRows();
    const audit = auditPath();

    const preview = await previewDrain(engine, { ...opts({ model: MODEL }), execute: false });
    const report = await runDrain(engine, opts({ model: MODEL }), { ...FAST, budgetAuditPath: audit });

    // The model that actually reached the (mocked) provider.
    expect(requested).toEqual([MODEL]);

    expect(preview.model_override).toBe(MODEL);
    expect(preview.blockers).toEqual([]);
    expect(preview.chat).toBe('available');
    expect(preview.pricing.chat).toBe('priced');
    expect(preview.would_attempt_at_most).toBe(1);

    expect(report.model_override).toBe(MODEL);
    expect(report.outcome).toBe('empty');
    expect(report.attempted).toBe(1);
    expect(report.durable.completed).toBe(1);
    expect(report.facts_inserted).toBe(1);
    expect(report.failures).toEqual({});

    // A persisted, source-bound fact.
    const facts = await factRows();
    expect(facts).toHaveLength(1);
    expect(facts[0]).toMatchObject({ source_id: 'default', visibility: 'private', context: slug });
    expect(String(facts[0]!.fact)).toContain('synthetic drain model fact');

    // Durable payload, submission authority and engine config are untouched.
    expect(await jobIdentity(id)).toBe(identityBefore);
    expect(await configRows()).toEqual(configBefore);

    // The drain tracker reserved and settled THIS model at the canonical rate.
    const lines = readFileSync(audit, 'utf8').trim().split('\n').map((l) => JSON.parse(l) as Record<string, unknown>);
    const chatReserve = lines.filter((l) => l.event === 'reserve' && l.kind === 'chat');
    const chatRecord = lines.filter((l) => l.event === 'record' && l.kind === 'chat');
    expect(chatReserve.map((l) => l.model)).toEqual([MODEL]);
    expect(chatRecord.map((l) => l.model)).toEqual([MODEL]);
    const rate = canonicalLookup(MODEL)!;
    expect(chatRecord[0]!.actual_cost_usd as number).toBeCloseTo((1000 * rate.input + 200 * rate.output) / 1e6, 12);
    expect(chatReserve[0]!.projected_cost_usd as number).toBeGreaterThanOrEqual(chatRecord[0]!.actual_cost_usd as number);
  });

  test('the override changes only the requested model: the stored fact equals an ordinary run on the same job', async () => {
    const run = async (model?: string) => {
      await fresh();
      const { id } = await factsJob(1, { model: PAYLOAD_MODEL });
      const identity = await jobIdentity(id);
      const report = await runDrain(engine, opts(model ? { model } : {}), FAST);
      return { report, identityAfter: await jobIdentity(id), identity, facts: await factRows(), models: [...requested] };
    };
    const ordinary = await run();
    const explicit = await run(MODEL);
    expect(ordinary.models).toEqual([PAYLOAD_MODEL]);
    expect(explicit.models).toEqual([MODEL]);
    expect(explicit.facts).toEqual(ordinary.facts);
    expect(explicit.identity).toBe(ordinary.identity);
    expect(explicit.identityAfter).toBe(explicit.identity);
    expect(explicit.report.durable).toEqual(ordinary.report.durable);
    expect(explicit.report.skipped).toEqual(ordinary.report.skipped);
  });

  test('the override does not outlive its run: a later ordinary drain uses the payload and configured models again', async () => {
    await fresh();
    await factsJob(1, { model: PAYLOAD_MODEL });
    await runDrain(engine, opts({ model: MODEL }), FAST);
    expect(requested).toEqual([MODEL]);

    chatStub();
    await factsJob(2, { model: 'google:gemini-2.5-flash-lite' });
    await factsJob(3);
    const later = await runDrain(engine, opts(), FAST);
    expect(requested).toEqual(['google:gemini-2.5-flash-lite', DEFAULT_MODEL]);
    expect(later.model_override).toBeNull();
    expect(await engine.getConfig('facts.extraction_model')).toBe(DEFAULT_MODEL);
  });
});

describe('without --model nothing changes', () => {
  test('a per-job payload model and the configured default are requested as before', async () => {
    await fresh();
    const a = await factsJob(1, { model: 'google:gemini-2.5-flash-lite' });
    const b = await factsJob(2);
    const identities = [await jobIdentity(a.id), await jobIdentity(b.id)];
    const report = await runDrain(engine, opts(), FAST);
    expect(requested).toEqual(['google:gemini-2.5-flash-lite', DEFAULT_MODEL]);
    expect(report.outcome).toBe('empty');
    expect(report.durable.completed).toBe(2);
    expect([await jobIdentity(a.id), await jobIdentity(b.id)]).toEqual(identities);
  });

  test('the preview and report say no override was applied', async () => {
    await fresh();
    await factsJob(1);
    const preview = await previewDrain(engine, { ...opts(), execute: false });
    expect(preview.model_override).toBeNull();
    const report = await runDrain(engine, opts(), FAST);
    expect(report.model_override).toBeNull();
  });
});

describe('the explicit model is what the preflight gates', () => {
  test('an unpriced explicit model blocks before any claim, although the configured default is priced', async () => {
    await fresh();
    await factsJob(1, { model: PAYLOAD_MODEL });
    const before = await allRows();
    const preview = await previewDrain(engine, { ...opts({ model: 'google:gemini-3.8-flash-latest' }), execute: false });
    expect(preview.pricing.chat).toBe('unpriced');
    expect(preview.blockers).toContain('unpriced_chat_model');
    const report = await runDrain(engine, opts({ model: 'google:gemini-3.8-flash-latest' }), FAST);
    expect(report.outcome).toBe('blocked');
    expect(requested).toEqual([]);
    expect(await allRows()).toEqual(before);
  });

  test('chat availability is checked for the explicit model, not the configured default', async () => {
    await fresh();
    __setChatTransportForTests(null);
    // Synthetic OpenAI key only: the openai default is servable, google 3.8 is not.
    configureGateway({ embedding_model: 'openai:text-embedding-3-small', embedding_dimensions: 1536, env: { OPENAI_API_KEY: 'sk-synthetic-drain-model' } });
    await engine.setConfig('facts.extraction_model', 'openai:gpt-5.2');
    await factsJob(1);
    const before = await allRows();
    const ordinary = await previewDrain(engine, { ...opts(), execute: false });
    expect(ordinary.chat).toBe('available');
    const preview = await previewDrain(engine, { ...opts({ model: MODEL }), execute: false });
    expect(preview.chat).toBe('unavailable');
    expect(preview.blockers).toContain('chat_unavailable');
    const report = await runDrain(engine, opts({ model: MODEL }), FAST);
    expect(report.outcome).toBe('blocked');
    expect(report.attempted).toBe(0);
    expect(await allRows()).toEqual(before);
  });

  test('an operator override that would replace the dated 3.8 rate is a visible blocker and is left in place', async () => {
    await fresh();
    await factsJob(1);
    const override = JSON.stringify({ [MODEL]: { input: 0.75, output: 3.75 } });
    await engine.setConfig('pricing.overrides', override);
    const before = await allRows();
    const preview = await previewDrain(engine, { ...opts({ model: MODEL }), execute: false });
    expect(preview.blockers).toContain('pricing_override_conflict');
    expect(preview.would_attempt_at_most).toBe(0);
    const report = await runDrain(engine, opts({ model: MODEL }), FAST);
    expect(report.outcome).toBe('blocked');
    expect(report.blockers).toContain('pricing_override_conflict');
    expect(requested).toEqual([]);
    expect(await allRows()).toEqual(before);
    expect(await engine.getConfig('pricing.overrides')).toBe(override);
  });

  test('an override that turns conflicting after the preview is judged again before any promotion or claim', async () => {
    await fresh();
    await factsJob(1);
    const due = await factsJob(2);
    await engine.executeRaw(`UPDATE minion_jobs SET status = 'delayed', delay_until = now() - interval '1 minute' WHERE id = $1`, [due.id]);
    const before = await allRows();
    const { result: report, reads } = await withPricingReads(
      async (n) => (n === 1 ? null : CONFLICT),
      () => runDrain(engine, opts({ model: MODEL }), FAST),
    );
    expect(reads).toBe(2);                       // the preview's read, then the execution snapshot
    expect(report.outcome).toBe('blocked');
    expect(report.blockers).toEqual(['pricing_override_conflict']);
    expect(report.promoted).toBe(0);
    expect(report.attempted).toBe(0);
    expect(requested).toEqual([]);
    expect(await allRows()).toEqual(before);
    expect(await engine.getConfig('pricing.overrides')).toBeNull();
  });

  test('the run prices with the snapshot it validated: a document changed later is never read or applied', async () => {
    await fresh();
    await factsJob(1);
    await factsJob(2);
    const audit = auditPath();
    const { result: report, reads } = await withPricingReads(
      async (n) => (n <= 2 ? null : CONFLICT),
      () => runDrain(engine, opts({ model: MODEL }), { ...FAST, budgetAuditPath: audit }),
    );
    expect(report.outcome).toBe('empty');
    expect(report.durable.completed).toBe(2);
    expect(reads).toBe(2);                       // nothing re-reads the document mid-run
    const rate = canonicalLookup(MODEL)!;
    const records = readFileSync(audit, 'utf8').trim().split('\n').map((l) => JSON.parse(l) as Record<string, unknown>)
      .filter((l) => l.event === 'record' && l.kind === 'chat');
    expect(records).toHaveLength(2);
    for (const l of records) expect(l.actual_cost_usd as number).toBeCloseTo((1000 * rate.input + 200 * rate.output) / 1e6, 12);
  });

  test('an unreadable override document blocks a pinned run before any claim; unpinned runs keep the fail-open reader', async () => {
    await fresh();
    await factsJob(1);
    await engine.setConfig('pricing.overrides', CONFLICT);   // would conflict, if it could be read
    const before = await allRows();
    const { result: preview } = await withPricingReads(unreadable, () => previewDrain(engine, { ...opts({ model: MODEL }), execute: false }));
    expect(preview.blockers).toEqual(['pricing_overrides_unreadable']);
    expect(preview.would_attempt_at_most).toBe(0);
    const { result: report } = await withPricingReads(unreadable, () => runDrain(engine, opts({ model: MODEL }), FAST));
    expect(report.outcome).toBe('blocked');
    expect(report.blockers).toEqual(['pricing_overrides_unreadable']);
    expect(requested).toEqual([]);
    expect(await allRows()).toEqual(before);
    expect(await engine.getConfig('pricing.overrides')).toBe(CONFLICT);
    expect(JSON.stringify(report)).not.toContain('synthetic pricing read failure');

    // Unpinned drains keep the legacy policy: a read failure means "no overrides".
    const { result: legacy } = await withPricingReads(unreadable, () => runDrain(engine, opts(), FAST));
    expect(legacy.outcome).toBe('empty');
    expect(legacy.blockers).toEqual([]);
    expect(requested).toEqual([DEFAULT_MODEL]);
  });

  test('a malformed document or malformed/ambiguous selected-model entry blocks a pinned run; nothing leaks or is rewritten', async () => {
    const malformed = [
      '{"google:gemini-3.8-flash": ',
      '[1, 2]',
      '"a string"',
      'null',
      JSON.stringify({ [MODEL]: 'free' }),
      JSON.stringify({ [MODEL]: { input: -1, output: 3.75 } }),
      JSON.stringify({ [MODEL]: { input: 1.5, output: 7.5 }, 'Google:Gemini-3.8-Flash': { input: 1.5, output: 7.5 } }),
    ];
    for (const doc of malformed) {
      await fresh();
      await factsJob(1);
      await engine.setConfig('pricing.overrides', doc);
      const preview = await previewDrain(engine, { ...opts({ model: MODEL }), execute: false });
      expect({ doc, blockers: preview.blockers }).toEqual({ doc, blockers: ['pricing_overrides_invalid'] });
      expect(await engine.getConfig('pricing.overrides')).toBe(doc);
    }
    // One full run on a malformed selected entry: blocked, no call, no leak.
    await fresh();
    await factsJob(1);
    const doc = JSON.stringify({ [MODEL]: { input: 'cheap', output: 0.0001 } });
    await engine.setConfig('pricing.overrides', doc);
    const before = await allRows();
    const report = await runDrain(engine, opts({ model: MODEL }), FAST);
    expect(report.outcome).toBe('blocked');
    expect(report.blockers).toEqual(['pricing_overrides_invalid']);
    expect(requested).toEqual([]);
    expect(await allRows()).toEqual(before);
    expect(JSON.stringify(report)).not.toContain('cheap');
    expect(await engine.getConfig('pricing.overrides')).toBe(doc);

    // Unpinned drains keep parsing leniently (policy unchanged).
    await engine.setConfig('pricing.overrides', '{"google:gemini-3.8-flash": ');
    expect((await previewDrain(engine, { ...opts(), execute: false })).blockers).toEqual([]);
  });

  test('other models\' entries, valid or not, keep the legacy handling in a pinned run', async () => {
    await fresh();
    await factsJob(1);
    const doc = JSON.stringify({ [DEFAULT_MODEL]: 'free', 'litellm:gpt-4o': { input: 2.5, output: 10 }, 'google/gemini-3.8-flash': 'free' });
    await engine.setConfig('pricing.overrides', doc);
    const preview = await previewDrain(engine, { ...opts({ model: MODEL }), execute: false });
    expect(preview.blockers).toEqual([]);
    const report = await runDrain(engine, opts({ model: MODEL }), FAST);
    expect(report.outcome).toBe('empty');
    expect(requested).toEqual([MODEL]);
    expect(await engine.getConfig('pricing.overrides')).toBe(doc);
  });

  test('the pinned snapshot is gated even with extraction disabled (extraction can be re-enabled mid-run)', async () => {
    await fresh();
    await engine.setConfig('facts.extraction_enabled', 'false');
    await factsJob(1);
    const before = await allRows();
    const { result: preview } = await withPricingReads(unreadable, () => previewDrain(engine, { ...opts({ model: MODEL }), execute: false }));
    expect(preview.extraction_enabled).toBe(false);
    expect(preview.blockers).toEqual(['pricing_overrides_unreadable']);
    const { result: report } = await withPricingReads(unreadable, () => runDrain(engine, opts({ model: MODEL }), FAST));
    expect(report.outcome).toBe('blocked');
    expect(report.attempted).toBe(0);
    expect(await allRows()).toEqual(before);
  });

  describe('execution re-applies chat and embedding priceability to its own snapshot', () => {
    const EMBED = 'litellm:review-unknown-embed';
    const CHAT = 'litellm:review-unknown-chat';
    let embedCalls = 0;
    async function setup(opts2: { embed?: string; chat?: string } = {}): Promise<{ before: string[] }> {
      await fresh();
      if (opts2.embed) configureGateway({ embedding_model: opts2.embed, embedding_dimensions: 1536, env: {} });
      if (opts2.chat) await engine.setConfig('facts.extraction_model', opts2.chat);
      embedCalls = 0;
      __setEmbedTransportForTests((async ({ values }: { values: string[] }) => {
        embedCalls++;
        return { embeddings: values.map(() => Array.from({ length: 1536 }, () => 0.1)) };
      }) as never);
      await factsJob(1);
      const due = await factsJob(2);
      await engine.executeRaw(`UPDATE minion_jobs SET status = 'delayed', delay_until = now() - interval '1 minute' WHERE id = $1`, [due.id]);
      return { before: await allRows() };
    }
    const factCount = async () => (await engine.executeRaw<{ n: number }>('SELECT count(*)::int AS n FROM facts'))[0]!.n;

    test('an embedding price that vanished after the preview blocks before any promotion, claim or provider call', async () => {
      const doc = JSON.stringify({ [EMBED]: 0.13 });
      const { before } = await setup({ embed: EMBED });
      // Read-only preview under the override: priced, nothing blocked, nothing changed.
      const { result: preview } = await withPricingReads(async () => doc, () => previewDrain(engine, { ...opts({ model: MODEL }), execute: false }));
      expect(preview.pricing.embedding).toBe('priced');
      expect(preview.blockers).toEqual([]);
      expect(await allRows()).toEqual(before);

      const { result: report, reads } = await withPricingReads(
        async (n) => (n === 1 ? doc : null),
        () => runDrain(engine, opts({ model: MODEL }), FAST),
      );
      expect(reads).toBe(2);
      expect(report.outcome).toBe('blocked');
      expect(report.blockers).toEqual(['unpriced_embedding_model']);
      expect([report.promoted, report.attempted, report.durable.completed]).toEqual([0, 0, 0]);
      expect(requested).toEqual([]);
      expect(embedCalls).toBe(0);
      expect(await allRows()).toEqual(before);
      expect(await factCount()).toBe(0);
    });

    test('a chat price that vanished after the preview blocks an unpinned run the same way', async () => {
      const doc = JSON.stringify({ [CHAT]: { input: 1, output: 1 } });
      const { before } = await setup({ chat: CHAT });
      const { result: report, reads } = await withPricingReads(
        async (n) => (n === 1 ? doc : null),
        () => runDrain(engine, opts(), FAST),
      );
      expect(reads).toBe(2);
      expect(report.outcome).toBe('blocked');
      expect(report.blockers).toEqual(['unpriced_chat_model']);
      expect([report.promoted, report.attempted]).toEqual([0, 0]);
      expect(requested).toEqual([]);
      expect(embedCalls).toBe(0);
      expect(await allRows()).toEqual(before);
      expect(await factCount()).toBe(0);
    });

    test('control: the same override present in both snapshots runs through the real handler and prices the embedding', async () => {
      const doc = JSON.stringify({ [EMBED]: 0.13 });
      await setup({ embed: EMBED });
      const audit = auditPath();
      const { result: report } = await withPricingReads(async () => doc,
        () => runDrain(engine, opts({ model: MODEL }), { ...FAST, budgetAuditPath: audit }));
      expect(report.outcome).toBe('empty');
      expect(report.promoted).toBe(1);
      expect(report.durable.completed).toBe(2);
      expect(requested).toEqual([MODEL, MODEL]);
      expect(embedCalls).toBeGreaterThan(0);
      const lines = readFileSync(audit, 'utf8').trim().split('\n').map((l) => JSON.parse(l) as Record<string, unknown>);
      expect(lines.some((l) => /unpriced|no_pricing/.test(String(l.event)))).toBe(false);
      expect(lines.filter((l) => l.kind === 'embed' && l.event === 'record').every((l) => l.model === EMBED)).toBe(true);
    });
  });

  test('overrides that do not touch the explicit model, or runs without --model, are not blocked by it', async () => {
    await fresh();
    await factsJob(1);
    await engine.setConfig('pricing.overrides', JSON.stringify({ [DEFAULT_MODEL]: { input: 9, output: 9 } }));
    const explicit = await previewDrain(engine, { ...opts({ model: MODEL }), execute: false });
    expect(explicit.blockers).toEqual([]);
    const ordinary = await previewDrain(engine, { ...opts(), execute: false });
    expect(ordinary.blockers).toEqual([]);
  });
});
