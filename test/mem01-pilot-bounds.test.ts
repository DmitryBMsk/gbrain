/**
 * MEM-01 pilot bounds for `gbrain jobs drain facts-absorb`.
 *
 *   - `--job-ids a,b,…` (1..4 distinct positive ids): the drain claims,
 *     promotes and observes ONLY those rows. Older / higher-priority foreign
 *     rows, foreign due delays and rows enqueued mid-run are never claimed or
 *     promoted and stay byte-identical. A selection with nothing claimable
 *     ends promptly. `MinionQueue.claim` / `promoteDueDelayed` take the same
 *     selector; an empty selector selects nothing (never a wildcard).
 *   - `--max-model-attempts N` (1..16): a per-run ceiling on chat provider
 *     attempts through the real gateway (retries and error attempts count).
 *     Attempt N+1 never reaches the transport, claiming stops, the refused
 *     claim is handed back without burning an attempt, and the cost cap and
 *     wall clock keep working alongside it.
 *   - Without either flag, argument parsing, SQL scope, SDK retry options and
 *     the JSON document shape are unchanged.
 *
 * PGLite in memory. Synthetic data and synthetic keys only; every provider
 * call goes through gateway test transports and `fetch` is a tripwire.
 */
import { afterAll, afterEach, beforeAll, describe, expect, test } from 'bun:test';
import { tmpdir } from 'node:os';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { MinionQueue } from '../src/core/minions/queue.ts';
import {
  chat,
  __setChatTransportForTests,
  __setEmbedTransportForTests,
  __setGenerateTextTransportForTests,
  configureGateway,
  resetGateway,
} from '../src/core/ai/gateway.ts';
import {
  DrainUsageError,
  parseDrainArgs,
  previewDrain,
  runDrain,
  type DrainDeps,
  type DrainOptions,
} from '../src/commands/jobs-drain.ts';
import { withAIInvocationPreflight } from '../src/core/ai/invocation-guard.ts';
import { emptyHome, withEnv } from './helpers/with-env.ts';

const MODEL = 'google:gemini-3.8-flash';
const LONG_BODY = 'synthetic pilot bounds meeting note that is comfortably longer than eighty characters '.repeat(3);
const FAST: DrainDeps = { pollMs: 50, workerPollMs: 25, graceMs: 400 };
const GOOD = JSON.stringify({ facts: [{ fact: 'synthetic pilot fact', kind: 'event', entity: null, confidence: 1, notability: 'high' }] });

let engine: PGLiteEngine;
const realFetch = globalThis.fetch;
let fetchCalls = 0;

beforeAll(async () => {
  // Early isolation is the preload's job; refuse to run if it did not happen.
  expect(process.env.GBRAIN_HOME?.startsWith(tmpdir())).toBe(true);
  globalThis.fetch = (async () => { fetchCalls++; throw new Error('network tripwire: no fetch in this suite'); }) as unknown as typeof fetch;
  engine = new PGLiteEngine();
  await engine.connect({});
  await engine.initSchema();
}, 120_000);
afterAll(async () => {
  globalThis.fetch = realFetch;
  await engine.disconnect();
  expect(fetchCalls).toBe(0);
});
afterEach(() => {
  __setChatTransportForTests(null);
  __setEmbedTransportForTests(null);
  __setGenerateTextTransportForTests(null);
  resetGateway();
  expect(fetchCalls).toBe(0);
});

const base = ['drain', 'facts-absorb', '--max-jobs', '4', '--max-usd', '1'];

describe('--job-ids argument contract (engine-free)', () => {
  test('a bounded csv of distinct positive ids, in either flag spelling', () => {
    expect(parseDrainArgs([...base, '--job-ids', '7']).jobIds).toEqual([7]);
    expect(parseDrainArgs([...base, '--job-ids=12,3,45,9']).jobIds).toEqual([12, 3, 45, 9]);
  });

  test('absent flag leaves the option absent (legacy scope)', () => {
    expect('jobIds' in parseDrainArgs(base)).toBe(false);
  });

  test('empty, malformed, duplicate, overlong or unsafe selections are rejected before any engine', () => {
    for (const v of ['', ',', '1,', ',1', '0', '-1', '1.5', '01', 'a', '1 2', '1,,2', '1,1', '1,2,3,4,5', '9007199254740992', '+1', '1e3']) {
      expect(() => parseDrainArgs([...base, `--job-ids=${v}`])).toThrow(DrainUsageError);
    }
    expect(() => parseDrainArgs([...base, '--job-ids'])).toThrow(DrainUsageError);
    expect(() => parseDrainArgs([...base, '--job-ids', '1', '--job-ids', '2'])).toThrow(DrainUsageError);
  });
});

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

async function fresh(): Promise<void> {
  await engine.executeRaw('TRUNCATE minion_jobs, facts, pages CASCADE');
  await engine.executeRaw(`DELETE FROM config WHERE key LIKE 'facts.%' OR key LIKE 'models.%' OR key = 'pricing.overrides'`);
  await engine.setConfig('facts.extraction_model', MODEL);
  __setEmbedTransportForTests((async ({ values }: { values: string[] }) =>
    ({ embeddings: values.map(() => Array.from({ length: 1536 }, () => 0.1)) })) as never);
}

let seq = 0;
interface Seed { priority?: number; ageMin?: number; delayed?: 'due' | 'future' }
async function factsJob(seed: Seed = {}): Promise<number> {
  const slug = `meetings/pilot-bounds-example-${++seq}`;
  await engine.putPage(slug, { type: 'meeting', title: `Pilot bounds example ${seq}`, compiled_truth: LONG_BODY }, { sourceId: 'default' } as never);
  const q = new MinionQueue(engine);
  const job = await q.add('facts-absorb',
    { slug, sourceId: 'default', source: 'mcp:put_page', notabilityFilter: 'all', visibility: 'private' },
    { queue: 'default', priority: seed.priority ?? 0, max_attempts: 5 });
  // Fixture shaping only (age / delay state); never an assertion channel.
  if (seed.ageMin) await engine.executeRaw(`UPDATE minion_jobs SET created_at = now() - ($2::int * interval '1 minute') WHERE id = $1`, [job.id, seed.ageMin]);
  if (seed.delayed) {
    await engine.executeRaw(`UPDATE minion_jobs SET status = 'delayed', delay_until = now() ${seed.delayed === 'due' ? '-' : '+'} interval '1 hour' WHERE id = $1`, [job.id]);
  }
  return job.id;
}

/** Byte-level identity of rows through the public getter. */
async function snapshot(ids: number[]): Promise<string[]> {
  const q = new MinionQueue(engine);
  return Promise.all(ids.map(async id => JSON.stringify(await q.getJob(id))));
}
async function statusOf(id: number): Promise<string | undefined> {
  return (await new MinionQueue(engine).getJob(id))?.status;
}

function opts(over: Partial<DrainOptions> = {}): DrainOptions & { execute: true } {
  return { name: 'facts-absorb', queue: 'default', maxJobs: 4, maxUsd: 1, maxRuntimeMs: 20_000, json: true, ...over, execute: true };
}

// ---------------------------------------------------------------------------
// Queue selector
// ---------------------------------------------------------------------------

describe('MinionQueue selector', () => {
  test('claim with a selector takes only a selected row, even behind older higher-priority rows', async () => {
    await fresh();
    const foreign = [await factsJob({ priority: -10, ageMin: 600 }), await factsJob({ ageMin: 300 })];
    const chosen = await factsJob();
    const before = await snapshot(foreign);
    const q = new MinionQueue(engine);
    const job = await q.claim('pilot-test:1', 30_000, 'default', ['facts-absorb'], { jobIds: [chosen] });
    expect(job?.id).toBe(chosen);
    expect(await q.claim('pilot-test:2', 30_000, 'default', ['facts-absorb'], { jobIds: [chosen] })).toBeNull();
    expect(await snapshot(foreign)).toEqual(before);
  });

  test('an empty selector claims nothing (never a wildcard); a nonmatching one claims nothing', async () => {
    await fresh();
    const foreign = [await factsJob(), await factsJob()];
    const before = await snapshot(foreign);
    const q = new MinionQueue(engine);
    expect(await q.claim('pilot-test:3', 30_000, 'default', ['facts-absorb'], { jobIds: [] })).toBeNull();
    expect(await q.claim('pilot-test:4', 30_000, 'default', ['facts-absorb'], { jobIds: [987_654] })).toBeNull();
    expect(await snapshot(foreign)).toEqual(before);
  });

  test('promoteDueDelayed with a selector promotes only selected due rows', async () => {
    await fresh();
    const foreignDue = await factsJob({ delayed: 'due', priority: -10 });
    const chosenDue = await factsJob({ delayed: 'due' });
    const chosenFuture = await factsJob({ delayed: 'future' });
    const before = await snapshot([foreignDue, chosenFuture]);
    const q = new MinionQueue(engine);
    expect(await q.promoteDueDelayed({ queue: 'default', name: 'facts-absorb', limit: 4, jobIds: [] })).toEqual([]);
    expect(await q.promoteDueDelayed({ queue: 'default', name: 'facts-absorb', limit: 4, jobIds: [chosenDue, chosenFuture] })).toEqual([chosenDue]);
    expect(await statusOf(chosenDue)).toBe('waiting');
    expect(await snapshot([foreignDue, chosenFuture])).toEqual(before);
  });

  test('control: without a selector claim keeps its legacy order (the older higher-priority row first)', async () => {
    await fresh();
    const older = await factsJob({ priority: -10, ageMin: 600 });
    await factsJob();
    expect((await new MinionQueue(engine).claim('pilot-test:5', 30_000, 'default', ['facts-absorb']))?.id).toBe(older);
  });
});

// ---------------------------------------------------------------------------
// Selected drain (real builtin facts-absorb handler)
// ---------------------------------------------------------------------------

let chatCalls = 0;
function chatStub(onCall?: () => Promise<void>): void {
  chatCalls = 0;
  __setChatTransportForTests(async () => {
    chatCalls++;
    await onCall?.();
    return {
      text: GOOD, blocks: [], stopReason: 'end',
      usage: { input_tokens: 1000, output_tokens: 200, cache_read_tokens: 0, cache_creation_tokens: 0 },
      model: MODEL, providerId: 'test',
    };
  });
}

describe('--job-ids drain', () => {
  test('only the selected rows are promoted, claimed and completed; foreign rows, including ones enqueued mid-run, are untouched', async () => {
    await fresh();
    const foreign = [
      await factsJob({ priority: -10, ageMin: 600 }),
      await factsJob({ priority: -5, ageMin: 500 }),
      await factsJob({ delayed: 'due', priority: -10 }),
      await factsJob({ delayed: 'future' }),
    ];
    const chosenWaiting = await factsJob();
    const chosenDue = await factsJob({ delayed: 'due' });
    const lateForeign: number[] = [];
    chatStub(async () => {
      if (lateForeign.length) return;
      lateForeign.push(await factsJob({ priority: -20, ageMin: 900 }), await factsJob({ delayed: 'due', priority: -20 }));
    });
    const before = await snapshot(foreign);
    const o = opts({ jobIds: [chosenWaiting, chosenDue] });

    const preview = await previewDrain(engine, { ...o, execute: false });
    expect(preview.job_ids).toEqual([chosenWaiting, chosenDue]);
    expect(preview.counts).toEqual({ waiting: 1, delayed: 1, delayed_due: 1, delayed_future: 0, active: 0, active_expired: 0 });
    expect(preview.would_attempt_at_most).toBe(2);

    const report = await runDrain(engine, o, FAST);
    expect(report.job_ids).toEqual([chosenWaiting, chosenDue]);
    expect(report.outcome).toBe('empty');           // scoped observation: foreign waiting rows do not keep it alive
    expect(report.promoted).toBe(1);
    expect(report.attempted).toBe(2);
    expect(report.durable.completed).toBe(2);
    expect(report.remaining).toEqual({ waiting: 0, delayed: 0, delayed_due: 0, delayed_future: 0, active: 0, active_expired: 0 });
    expect(chatCalls).toBe(2);
    expect(await statusOf(chosenWaiting)).toBe('completed');
    expect(await statusOf(chosenDue)).toBe('completed');
    expect(await snapshot(foreign)).toEqual(before);
    expect(lateForeign).toHaveLength(2);
    expect(await statusOf(lateForeign[0]!)).toBe('waiting');
    expect(await statusOf(lateForeign[1]!)).toBe('delayed');
  });

  test('a selection with nothing claimable ends promptly without touching any row', async () => {
    await fresh();
    chatStub();
    const foreign = [await factsJob({ priority: -10 }), await factsJob({ delayed: 'due' })];
    const done = await factsJob();
    const before = await snapshot(foreign);
    await runDrain(engine, opts({ jobIds: [done] }), FAST);
    expect(await statusOf(done)).toBe('completed');
    chatStub();
    const doneBefore = await snapshot([done]);
    const started = Date.now();
    const report = await runDrain(engine, opts({ jobIds: [done, 987_654] }), FAST);
    expect(report.outcome).toBe('empty');
    expect([report.promoted, report.attempted]).toEqual([0, 0]);
    expect(Date.now() - started).toBeLessThan(5_000);
    expect(chatCalls).toBe(0);
    expect(await snapshot(foreign)).toEqual(before);   // a waiting and a due foreign row, across both runs
    expect(await snapshot([done])).toEqual(doneBefore);
  });

  test('without --job-ids the preview and report keep exactly the legacy document keys', async () => {
    await fresh();
    chatStub();
    await factsJob();
    const preview = await previewDrain(engine, { ...opts(), execute: false });
    expect(Object.keys(preview)).toEqual([
      'schema_version', 'mode', 'name', 'queue', 'model_override', 'counts', 'oldest_waiting_age_seconds',
      'extraction_enabled', 'chat', 'embedding', 'pricing', 'blockers', 'would_promote_at_most', 'would_attempt_at_most',
    ]);
    const report = await runDrain(engine, opts(), FAST);
    expect(Object.keys(report)).toEqual([
      'schema_version', 'mode', 'name', 'queue', 'model_override', 'outcome', 'blockers', 'extraction_enabled',
      'promoted', 'attempted', 'handler_returned', 'handler_threw', 'durable', 'released_unrun',
      'domain_stop_confirmed', 'in_flight_unconfirmed', 'skipped', 'failures', 'facts_inserted', 'remaining',
      'budget', 'elapsed_ms', 'max_runtime_ms',
    ]);
    expect(Object.keys(report.budget)).toEqual(['max_usd', 'spent_usd', 'exhausted', 'final']);
  });
});

// ---------------------------------------------------------------------------
// Opt-in model identity (metadata only)
// ---------------------------------------------------------------------------

describe('opt-in documents name the model identifiers they can confirm', () => {
  const EMBED = 'openai:text-embedding-3-small';
  /** Gateway embedding + the brain's stored `embedding_model`; restored afterwards. */
  async function withEmbedding<T>(stored: string, fn: () => Promise<T>): Promise<T> {
    configureGateway({ embedding_model: EMBED, embedding_dimensions: 1536, env: { OPENAI_API_KEY: 'sk-synthetic-pilot' } });
    await fresh();
    chatStub();
    const prior = await engine.getConfig('embedding_model');
    await engine.setConfig('embedding_model', stored);
    try { return await fn(); } finally { if (prior !== null) await engine.setConfig('embedding_model', prior); }
  }

  test('a pinned selected run reports the requested chat model and the embedding model both planes agree on', async () => {
    await withEmbedding(EMBED, async () => {
      const id = await factsJob();
      const preview = await previewDrain(engine, { ...opts({ jobIds: [id], model: MODEL }), execute: false });
      expect(preview.models).toEqual({ chat: MODEL, embedding: EMBED });
      expect(chatCalls).toBe(0);                                  // metadata only
      const attempts = await previewDrain(engine, { ...opts({ maxModelAttempts: 2, model: MODEL }), execute: false });
      expect(attempts.models).toEqual({ chat: MODEL, embedding: EMBED });
      const report = await keyed(() => runDrain(engine, opts({ jobIds: [id], model: MODEL }), FAST));
      expect(report.models).toEqual({ chat: MODEL, embedding: EMBED });
    });
  });

  test('identifiers that cannot be confirmed are null, never guessed', async () => {
    await withEmbedding('openai:text-embedding-3-large', async () => {
      const id = await factsJob();
      // Unpinned: queued payload models may win over the configured default.
      const unpinned = await previewDrain(engine, { ...opts({ jobIds: [id] }), execute: false });
      expect(unpinned.models).toEqual({ chat: null, embedding: null });   // stored model disagrees with the gateway
      await engine.setConfig('facts.extraction_enabled', 'false');
      const disabled = await previewDrain(engine, { ...opts({ jobIds: [id], model: MODEL }), execute: false });
      expect(disabled.models).toEqual({ chat: null, embedding: null });   // no extraction request is made
      expect(chatCalls).toBe(0);
    });
  });

  test('without --job-ids / --max-model-attempts no model identity is added, pinned or not', async () => {
    await withEmbedding(EMBED, async () => {
      await factsJob();
      expect('models' in await previewDrain(engine, { ...opts({ model: MODEL }), execute: false })).toBe(false);
      expect('models' in await previewDrain(engine, { ...opts(), execute: false })).toBe(false);
    });
  });
});

// ---------------------------------------------------------------------------
// --max-model-attempts through the real gateway (fake SDK transport only)
// ---------------------------------------------------------------------------

describe('--max-model-attempts argument contract (engine-free)', () => {
  const pinned = [...base, '--model', MODEL];

  test('an integer from 1 to 16, in either spelling, with an explicit native Google model; absent = no ceiling', () => {
    expect(parseDrainArgs([...pinned, '--max-model-attempts', '1']).maxModelAttempts).toBe(1);
    expect(parseDrainArgs([...pinned, '--max-model-attempts=16']).maxModelAttempts).toBe(16);
    expect('maxModelAttempts' in parseDrainArgs(base)).toBe(false);
  });

  test('out-of-range, malformed or duplicated values are rejected', () => {
    for (const v of ['0', '17', '-1', '1.5', 'a', '', '2x']) {
      expect(() => parseDrainArgs([...pinned, `--max-model-attempts=${v}`])).toThrow(DrainUsageError);
    }
    expect(() => parseDrainArgs([...pinned, '--max-model-attempts'])).toThrow(DrainUsageError);
    expect(() => parseDrainArgs([...pinned, '--max-model-attempts', '2', '--max-model-attempts', '3'])).toThrow(DrainUsageError);
  });

  test('outside the supported scope (unpinned, or a pin other than native google:) the cap is rejected', () => {
    for (const model of [null, 'openai:gpt-5.2', 'anthropic:claude-sonnet-4-6', 'openrouter:google/gemini-3.8-flash']) {
      const args = [...base, ...(model ? ['--model', model] : []), '--max-model-attempts', '2'];
      expect(() => parseDrainArgs(args)).toThrow(/--max-model-attempts requires --model google:/);
    }
  });
});

type Reply = 'good' | 'malformed' | 'error' | 'hang';
const sdkCalls: Array<{ maxRetries: unknown }> = [];
/** Synthetic keys only. The builtin handler's per-job gateway refresh re-reads
 *  process env + an (empty) file plane, so they are scoped with withEnv. */
const KEYS = { GOOGLE_GENERATIVE_AI_API_KEY: 'synthetic-pilot-google-key', OPENAI_API_KEY: 'sk-synthetic-pilot' };
const keyed = <T>(fn: () => Promise<T>) => withEnv({ ...KEYS, GBRAIN_HOME: emptyHome() }, fn);
/** Real gateway chat() → guarded generation → this fake SDK transport. */
async function realGateway(replies: (n: number) => Reply): Promise<void> {
  configureGateway({ embedding_model: 'openai:text-embedding-3-small', embedding_dimensions: 1536, env: { ...KEYS } });
  await fresh();
  sdkCalls.length = 0;
  __setGenerateTextTransportForTests((async (args: { maxRetries?: unknown; abortSignal?: AbortSignal }) => {
    sdkCalls.push({ maxRetries: args.maxRetries });
    const reply = replies(sdkCalls.length);
    if (reply === 'error') throw new Error('synthetic provider failure');
    if (reply === 'hang') {
      await new Promise((_, reject) => {
        const timer = setTimeout(() => reject(new Error('synthetic hang bound')), 5_000);
        args.abortSignal?.addEventListener('abort', () => { clearTimeout(timer); reject(args.abortSignal!.reason ?? new Error('aborted')); });
      });
    }
    return { content: [{ type: 'text', text: reply === 'good' ? GOOD : 'not json at all' }], finishReason: 'stop', usage: { inputTokens: 1000, outputTokens: 200 } };
  }) as never);
}
/** Every job's first reply is malformed, so the extractor retries once. */
const malformedThenGood = (n: number): Reply => (n % 2 === 1 ? 'malformed' : 'good');
/** The supported cap: always an explicit native Google pin. */
const capped = (maxModelAttempts: number, over: Partial<DrainOptions> = {}) => opts({ maxModelAttempts, model: MODEL, ...over });

describe('--max-model-attempts supported scope (programmatic)', () => {
  test('runDrain blocks an unpinned or non-Google capped run before promotion, claim or provider call', async () => {
    for (const model of [undefined, 'openai:gpt-5.2']) {
      await realGateway(() => 'good');
      const ids = [await factsJob(), await factsJob({ delayed: 'due' })];
      const before = await snapshot(ids);
      const o = opts({ maxModelAttempts: 2, ...(model ? { model } : {}) });
      const preview = await keyed(() => previewDrain(engine, { ...o, execute: false }));
      expect(preview.blockers).toContain('model_attempts_unsupported_model');
      expect(preview.would_attempt_at_most).toBe(0);
      const report = await keyed(() => runDrain(engine, o, FAST));
      expect(report.outcome).toBe('blocked');
      expect(report.blockers).toContain('model_attempts_unsupported_model');
      expect([report.promoted, report.attempted]).toEqual([0, 0]);
      expect(sdkCalls).toHaveLength(0);
      expect(await snapshot(ids)).toEqual(before);
    }
  });
});

describe('--max-model-attempts provider-family guard', () => {
  test('a chat invocation outside the pinned native Google family is refused before the transport and nothing more is claimed', async () => {
    await fresh();
    chatStub();
    const [a, b] = [await factsJob({ ageMin: 2 }), await factsJob({ ageMin: 1 })];
    const untouched = await snapshot([b]);
    // Seam: a handler that (unexpectedly) routes to another provider family through the real gateway.
    const registerHandlers: DrainDeps['registerHandlers'] = async (w) => {
      w.register('facts-absorb', async () => {
        await chat({ model: 'anthropic:claude-sonnet-4-6', messages: [{ role: 'user', content: 'synthetic' }] });
        return { inserted: 0 };
      });
    };
    const report = await runDrain(engine, capped(2), { ...FAST, registerHandlers });
    expect(chatCalls).toBe(0);
    expect(report.outcome).toBe('blocked');
    expect(report.blockers).toEqual(['model_provider_mismatch']);
    expect(report.model_attempts).toEqual({ attempted: 0, max: 2, exhausted: false });
    expect(report.durable).toMatchObject({ deferred: 1, completed: 0, retry_scheduled: 0 });
    const refused = await new MinionQueue(engine).getJob(a);
    expect([refused?.status, refused?.attempts_made]).toEqual(['delayed', 0]);
    expect(await snapshot([b])).toEqual(untouched);
  });
});

describe('uncapped drain SDK retries', () => {
  test('without --max-model-attempts the SDK still makes no hidden retries beyond the --max-usd reservation', async () => {
    for (const model of [undefined, MODEL]) {
      await realGateway(() => 'good');
      await factsJob();
      const report = await keyed(() => runDrain(engine, opts(model ? { model } : {}), FAST));
      expect(report.durable.completed).toBe(1);
      expect(sdkCalls.length).toBeGreaterThan(0);
      expect(sdkCalls.map(x => x.maxRetries)).toEqual(sdkCalls.map(() => 0));
    }
  });
});

describe('--max-model-attempts drain', () => {
  test('retries count; attempt N+1 never reaches the transport, claiming stops, the refused claim is deferred', async () => {
    await realGateway(malformedThenGood);
    const [a, b, c] = [await factsJob({ ageMin: 3 }), await factsJob({ ageMin: 2 }), await factsJob({ ageMin: 1 })];
    const untouched = await snapshot([c]);

    const preview = await keyed(() => previewDrain(engine, { ...capped(3), execute: false }));
    expect(preview.max_model_attempts).toBe(3);
    expect(preview.blockers).toEqual([]);

    const report = await keyed(() => runDrain(engine, capped(3), FAST));
    expect(sdkCalls).toHaveLength(3);                         // a: malformed+good, b: malformed, b's retry refused
    expect(sdkCalls.every(x => x.maxRetries === 0)).toBe(true); // no hidden SDK retries behind the counter
    expect(report.outcome).toBe('model_attempts_exhausted');
    expect(report.model_attempts).toEqual({ attempted: 3, max: 3, exhausted: true });
    expect(report.attempted).toBe(2);
    expect(report.durable.completed).toBe(1);
    expect(report.durable.deferred).toBe(1);
    expect(report.facts_inserted).toBe(1);
    expect(report.budget.final).toBe(true);
    expect(report.budget.spent_usd).toBeGreaterThan(0);
    expect(report.domain_stop_confirmed).toBe(true);
    expect(await statusOf(a)).toBe('completed');
    const refused = await new MinionQueue(engine).getJob(b);
    expect(refused?.status).toBe('delayed');
    expect(refused?.attempts_made).toBe(0);
    expect(await snapshot([c])).toEqual(untouched);             // never claimed after the halt
  });

  test('the last admitted attempt settles normally and nothing more is claimed', async () => {
    await realGateway(() => 'good');
    const [a, b] = [await factsJob({ ageMin: 2 }), await factsJob({ ageMin: 1 })];
    const untouched = await snapshot([b]);
    const report = await keyed(() => runDrain(engine, capped(1), FAST));
    expect(sdkCalls).toHaveLength(1);
    expect(report.model_attempts).toEqual({ attempted: 1, max: 1, exhausted: true });
    expect(report.outcome).toBe('model_attempts_exhausted');
    expect([report.attempted, report.released_unrun]).toEqual([1, 0]);
    expect(report.durable).toMatchObject({ completed: 1, deferred: 0, released: 0 });
    expect(report.budget.final).toBe(true);
    expect(await statusOf(a)).toBe('completed');
    expect(await snapshot([b])).toEqual(untouched);
  });

  test('failed provider attempts count toward the ceiling', async () => {
    await realGateway((n) => (n === 1 ? 'error' : 'good'));
    const [a, b, c] = [await factsJob({ ageMin: 3 }), await factsJob({ ageMin: 2 }), await factsJob({ ageMin: 1 })];
    const untouched = await snapshot([c]);
    const report = await keyed(() => runDrain(engine, capped(2), FAST));
    expect(sdkCalls).toHaveLength(2);
    expect(report.model_attempts).toEqual({ attempted: 2, max: 2, exhausted: true });
    expect(report.outcome).toBe('model_attempts_exhausted');
    expect(report.durable).toMatchObject({ retry_scheduled: 1, completed: 1, deferred: 0 });
    expect(await statusOf(a)).toBe('delayed');
    expect(await statusOf(b)).toBe('completed');
    expect(await snapshot([c])).toEqual(untouched);             // the ceiling was spent: never claimed
  });

  test('an outer invocation preflight still admits every sent attempt and never sees the refused one', async () => {
    await realGateway(malformedThenGood);
    await factsJob({ ageMin: 2 });
    await factsJob({ ageMin: 1 });
    const outer: string[] = [];
    const report = await keyed(() => withAIInvocationPreflight(
      async (call) => { if (call.kind === 'chat') outer.push(call.model); },
      () => runDrain(engine, capped(3), FAST)));
    expect(sdkCalls).toHaveLength(3);
    expect(outer).toEqual([MODEL, MODEL, MODEL]);
    expect(report.model_attempts).toEqual({ attempted: 3, max: 3, exhausted: true });
  });

  test('a parent refusal is never bypassed: nothing is sent or spent, and `attempted` stays a conservative upper bound', async () => {
    await realGateway(() => 'good');
    const id = await factsJob();
    let parentSaw = 0;
    const report = await keyed(() => withAIInvocationPreflight(
      async (call) => { if (call.kind === 'chat') { parentSaw++; throw new Error('synthetic parent refusal'); } },
      () => runDrain(engine, capped(2), FAST)));
    expect(parentSaw).toBe(1);                                    // the parent was consulted
    expect(sdkCalls).toHaveLength(0);                             // and its refusal held
    expect(report.budget.spent_usd).toBe(0);
    // Admitted by the drain before the parent refused: counted, although nothing was sent.
    expect(report.model_attempts).toEqual({ attempted: 1, max: 2, exhausted: false });
    expect(report.durable.retry_scheduled).toBe(1);
    expect(await statusOf(id)).toBe('delayed');
  });

  test('the cost cap still applies alongside the attempt ceiling', async () => {
    await realGateway(() => 'good');
    await factsJob();
    const report = await keyed(() => runDrain(engine, capped(16, { maxUsd: 0.000001 }), FAST));
    expect(report.outcome).toBe('budget_exhausted');
    expect(report.budget.exhausted).toBe(true);
    expect(sdkCalls).toHaveLength(0);
    expect(report.model_attempts).toEqual({ attempted: 0, max: 16, exhausted: false });
  });

  test('the wall clock still applies alongside the attempt ceiling', async () => {
    await realGateway(() => 'hang');
    await factsJob();
    const report = await keyed(() => runDrain(engine, capped(16, { maxRuntimeMs: 1_500 }), FAST));
    expect(report.outcome).toBe('time_limit');
    expect(sdkCalls).toHaveLength(1);
    expect(report.model_attempts).toEqual({ attempted: 1, max: 16, exhausted: false });
  }, 30_000);

  test('control: without the flag the same run keeps its extractor retries and report shape', async () => {
    await realGateway(malformedThenGood);
    const ids = [await factsJob({ ageMin: 3 }), await factsJob({ ageMin: 2 }), await factsJob({ ageMin: 1 })];
    const report = await keyed(() => runDrain(engine, opts(), FAST));
    expect(report.outcome).toBe('empty');
    expect(sdkCalls).toHaveLength(6);
    expect(report.durable.completed).toBe(3);
    expect('model_attempts' in report).toBe(false);
    for (const id of ids) expect(await statusOf(id)).toBe('completed');
  });
});
