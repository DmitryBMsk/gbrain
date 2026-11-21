/**
 * MEM-01 — `gbrain_feed` source-contract parser + extractor wiring.
 *
 * Parser: strict, fail-closed v1 shapes (real calendar dates, canonical UTC
 * timestamps, exact weekly window, producer/kind pairs, no unknown keys).
 * Extractor: without a contract the conversation prompt, user content and
 * outputs are byte-identical; with one, the rules + data block are added and
 * every fact is labelled (label kept whole under truncation, applied after the
 * junk gate and before embedding), dated to source time and attributed only
 * to the contract subject. Gateway test transports only — no key, no network.
 */

import { afterAll, beforeEach, describe, expect, test } from 'bun:test';
import {
  configureGateway,
  resetGateway,
  __setChatTransportForTests,
  __setEmbedTransportForTests,
  type ChatOpts,
  type ChatResult,
} from '../src/core/ai/gateway.ts';
import {
  buildExtractorSystem,
  extractFactsFromTurnWithOutcome,
} from '../src/core/facts/extract.ts';
import {
  SOURCE_CONTRACT_RULES,
  composeContractFact,
  parseCalendarDate,
  parseFeedContract,
  parseUtcTimestamp,
  type FeedContract,
} from '../src/core/facts/source-contract.ts';
import type { BrainEngine } from '../src/core/engine.ts';

function ok(frontmatter: Record<string, unknown>): FeedContract {
  const r = parseFeedContract(frontmatter);
  if (r.status !== 'ok') throw new Error(`expected ok, got ${JSON.stringify(r)}`);
  return r.contract;
}

const feed = (v: Record<string, unknown>) => ({ gbrain_feed: v });
const completionLog = { contract: 1, producer: 'todoist', kind: 'task_completion_log', day: '2026-07-23' };

describe('parseFeedContract', () => {
  test('absent only when the key is missing', () => {
    expect(parseFeedContract(undefined)).toEqual({ status: 'absent' });
    expect(parseFeedContract(null)).toEqual({ status: 'absent' });
    expect(parseFeedContract({ type: 'note', title: 'x' })).toEqual({ status: 'absent' });
    for (const v of [null, 'yes', 1, ['a'], new Date('2026-07-23T00:00:00Z')]) {
      expect(parseFeedContract({ gbrain_feed: v }).status).toBe('invalid');
    }
  });

  test('each v1 kind: anchor time, label, claim strength', () => {
    const log = ok(feed(completionLog));
    expect([log.anchorTime.toISOString(), log.label, log.claimStrength, log.subject])
      .toEqual(['2026-07-23T00:00:00.000Z', '[Todoist tracker completion status, UTC day 2026-07-23]', 'recorded_status', null]);

    const snap = ok(feed({ contract: 1, producer: 'todoist', kind: 'task_snapshot', observed_at: '2026-07-23T12:00:00.123456Z' }));
    expect([snap.anchorTime.toISOString(), snap.label])
      .toEqual(['2026-07-23T12:00:00.123Z', '[Todoist open-task snapshot at 2026-07-23T12:00:00.123456Z]']);

    const daily = ok(feed({ contract: 1, producer: 'health', kind: 'health_daily', measured_on: '2024-02-29', subject: 'people/example-owner' }));
    expect([daily.anchorTime.toISOString(), daily.label, daily.claimStrength, daily.subject])
      .toEqual(['2024-02-29T00:00:00.000Z', '[Health device record, measured on 2024-02-29]', 'measured', 'people/example-owner']);

    const weekly = ok(feed({ contract: 1, producer: 'health', kind: 'health_weekly', window_start: '2025-12-29', window_end: '2026-01-04' }));
    expect([weekly.anchorTime.toISOString(), weekly.label, weekly.claimStrength])
      .toEqual(['2025-12-29T00:00:00.000Z', '[Health device weekly summary, window 2025-12-29..2026-01-04]', 'aggregate']);
  });

  test('strict calendar dates', () => {
    for (const good of ['2024-02-29', '2026-12-31', '2026-01-01']) expect(parseCalendarDate(good)).not.toBeNull();
    for (const bad of ['2026-02-30', '2025-02-29', '2026-13-01', '2026-00-10', '2026-04-31', '2026-7-23', '20260723', ' 2026-07-23', 20260723, null]) {
      expect(parseCalendarDate(bad)).toBeNull();
    }
  });

  test('canonical UTC timestamps only', () => {
    expect(parseUtcTimestamp('2026-07-23T23:59:59Z')?.toISOString()).toBe('2026-07-23T23:59:59.000Z');
    expect(parseUtcTimestamp('2026-07-23T00:00:00.5Z')?.toISOString()).toBe('2026-07-23T00:00:00.500Z');
    for (const bad of [
      '2026-07-23T24:00:00Z', '2026-07-23T12:60:00Z', '2026-07-23T12:00:60Z', '2026-02-30T12:00:00Z',
      '2026-07-23T12:00:00', '2026-07-23T12:00:00+00:00', '2026-07-23T12:00:00+03:00', '2026-07-23 12:00:00Z',
      '2026-07-23T12:00:00.1234567Z', '2026-07-23T12:00Z',
    ]) {
      expect(parseUtcTimestamp(bad)).toBeNull();
    }
  });

  test('fail-closed reasons', () => {
    const reason = (v: Record<string, unknown>) => {
      const r = parseFeedContract(feed(v));
      return r.status === 'invalid' ? r.reason : r.status;
    };
    expect(reason({ ...completionLog, contract: 2 })).toBe('unsupported_version');
    expect(reason({ ...completionLog, contract: '1' })).toBe('unsupported_version');
    expect(reason({ ...completionLog, kind: 'calendar_event' })).toBe('unknown_kind');
    expect(reason({ ...completionLog, producer: 'health' })).toBe('producer_kind_mismatch');
    expect(reason({ ...completionLog, note: 'x' })).toBe('unknown_key:note');
    expect(reason({ ...completionLog, observed_at: '2026-07-23T00:00:00Z' })).toBe('unknown_key:observed_at');
    expect(reason({ contract: 1, producer: 'todoist', kind: 'task_completion_log' })).toBe('bad_date:day');
    expect(reason({ contract: 1, producer: 'health', kind: 'health_weekly', window_start: '2026-08-23', window_end: '2026-08-17' })).toBe('window_inverted');
    expect(reason({ contract: 1, producer: 'health', kind: 'health_weekly', window_start: '2026-08-17', window_end: '2026-08-24' })).toBe('window_not_seven_days');
    for (const subject of ['People/x', 'x', 'people/../x', 'people/a b', '/people/x', 'people/x\\y', 7]) {
      expect(reason({ ...completionLog, subject })).toBe('bad_subject');
    }
  });

  test('composeContractFact keeps the whole label within the limit', () => {
    const c = ok(feed(completionLog));
    expect(composeContractFact(c, 'short')).toBe(`${c.label} short`);
    const long = composeContractFact(c, 'x'.repeat(600));
    expect(long.length).toBe(500);
    expect(long.startsWith(`${c.label} x`)).toBe(true);
    expect(long.endsWith('...')).toBe(true);
    const exact = 'y'.repeat(500 - c.label.length - 1);
    expect(composeContractFact(c, exact)).toBe(`${c.label} ${exact}`);
  });
});

// ---------------------------------------------------------------------------

function chatResult(facts: Array<Record<string, unknown>>): ChatResult {
  const text = JSON.stringify({ facts });
  return {
    text,
    blocks: [{ type: 'text', text }],
    stopReason: 'end',
    usage: { input_tokens: 1, output_tokens: 1, cache_read_tokens: 0, cache_creation_tokens: 0 },
    model: 'anthropic:claude-sonnet-4-6',
    providerId: 'anthropic',
  } as ChatResult;
}

function stubEngine(config: Record<string, string>): BrainEngine {
  return { getConfig: async (key: string) => config[key] ?? null } as unknown as BrainEngine;
}

let seen: ChatOpts[] = [];
function stubChat(facts: Array<Record<string, unknown>>): void {
  seen = [];
  __setChatTransportForTests(async (opts) => {
    seen.push(opts);
    return chatResult(facts);
  });
}

beforeEach(() => {
  resetGateway();
  __setChatTransportForTests(null);
  __setEmbedTransportForTests(null);
  configureGateway({ chat_model: 'anthropic:claude-sonnet-4-6', env: { ANTHROPIC_API_KEY: 'sk-ant-test' } });
});

afterAll(() => {
  __setChatTransportForTests(null);
  __setEmbedTransportForTests(null);
  configureGateway({ embedding_model: 'openai:text-embedding-3-large', embedding_dimensions: 1536, env: { ...process.env } });
});

const TURN = 'I gave up alcohol and alice-example moved to Acme.';
const CANDIDATES = [
  { fact: 'gave up alcohol', kind: 'commitment', entity: 'people/alice-example', notability: 'high' },
  { fact: 'moved to Acme', kind: 'event', entity: 'people/alice-example', notability: 'medium' },
];

describe('extractor without a contract is unchanged', () => {
  for (const [label, admission, admitsLow] of [
    ['no admission', undefined, true],
    ['high-only admission', { allowed: ['high'] as const, invalid: 'drop' as const }, false],
  ] as const) {
    test(`prompt, user content and outputs (${label})`, async () => {
      stubChat(CANDIDATES);
      const out = await extractFactsFromTurnWithOutcome({
        turnText: TURN, source: 'mcp:extract_facts', engine: stubEngine({}), embedding: null,
        ...(admission ? { notabilityAdmission: admission } : {}),
      });
      expect(seen).toHaveLength(1);
      expect(seen[0].system).toBe(buildExtractorSystem(admitsLow));
      expect(seen[0].messages[0].content).toBe(`<turn>\n${TURN}\n</turn>\n\nExtract up to 10 facts.`);
      if (!out.ok) throw new Error('extraction failed');
      expect(out.facts.map(f => [f.fact, f.entity_slug, 'valid_from' in f])).toEqual(
        (admission ? CANDIDATES.slice(0, 1) : CANDIDATES).map(c => [c.fact, c.entity, false]),
      );
    });
  }
});

describe('extractor with a contract', () => {
  const contract = ok(feed({ ...completionLog, subject: 'people/example-owner' }));

  test('rules then operator appendix; data block before the turn', async () => {
    stubChat(CANDIDATES);
    await extractFactsFromTurnWithOutcome({
      turnText: TURN, source: 'mcp:put_page', embedding: null, sourceContract: contract,
      engine: stubEngine({ 'facts.extraction_prompt_appendix': 'OPERATOR APPENDIX' }),
    });
    expect(seen[0].system).toBe(`${buildExtractorSystem(true)}\n\n${SOURCE_CONTRACT_RULES}\n\nOPERATOR APPENDIX`);
    expect(seen[0].messages[0].content).toBe(
      '<source_contract>{"producer":"todoist","kind":"task_completion_log","claim_strength":"recorded_status","day":"2026-07-23","time_zone":"UTC"}</source_contract>\n'
      + `<turn>\n${TURN}\n</turn>\n\nExtract up to 10 facts.`,
    );
  });

  test('label, source time and contract subject on every fact; model entity ignored', async () => {
    stubChat(CANDIDATES);
    const out = await extractFactsFromTurnWithOutcome({
      turnText: TURN, source: 'mcp:put_page', engine: stubEngine({}), embedding: null, sourceContract: contract,
    });
    if (!out.ok) throw new Error('extraction failed');
    expect(out.facts.map(f => [f.fact, f.entity_slug, f.valid_from?.toISOString()])).toEqual([
      [`${contract.label} gave up alcohol`, 'people/example-owner', '2026-07-23T00:00:00.000Z'],
      [`${contract.label} moved to Acme`, 'people/example-owner', '2026-07-23T00:00:00.000Z'],
    ]);
  });

  test('junk gate judges the claim, admission still applies, long claims keep the label', async () => {
    stubChat([
      { fact: 'Let me check the tracker export first', kind: 'fact', notability: 'high' },
      { fact: 'low tier detail', kind: 'fact', notability: 'low' },
      { fact: 'z'.repeat(700), kind: 'fact', notability: 'high' },
    ]);
    const out = await extractFactsFromTurnWithOutcome({
      turnText: TURN, source: 'mcp:put_page', engine: stubEngine({}), embedding: null, sourceContract: contract,
      notabilityAdmission: { allowed: ['high', 'medium'], invalid: 'drop' },
    });
    if (!out.ok) throw new Error('extraction failed');
    expect(out.facts).toHaveLength(1);
    expect(out.facts[0].fact.length).toBe(500);
    expect(out.facts[0].fact.startsWith(`${contract.label} zzz`)).toBe(true);
  });

  test('the labelled text is what gets embedded', async () => {
    configureGateway({
      chat_model: 'anthropic:claude-sonnet-4-6', embedding_model: 'openai:text-embedding-3-small', embedding_dimensions: 1536,
      env: { ANTHROPIC_API_KEY: 'sk-ant-test', OPENAI_API_KEY: 'test' },
    });
    const embedded: string[] = [];
    __setEmbedTransportForTests((async ({ values }: { values: string[] }) => {
      embedded.push(...values);
      return { embeddings: values.map(() => Array.from({ length: 1536 }, () => 0.1)) };
    }) as never);
    stubChat(CANDIDATES.slice(0, 1));
    const out = await extractFactsFromTurnWithOutcome({
      turnText: TURN, source: 'mcp:put_page', engine: stubEngine({}), sourceContract: contract,
    });
    if (!out.ok) throw new Error('extraction failed');
    expect(embedded).toEqual([`${contract.label} gave up alcohol`]);
    expect(out.facts[0].embedding).not.toBeNull();
  });

  test('abort still propagates', async () => {
    stubChat(CANDIDATES);
    const ac = new AbortController();
    ac.abort();
    await expect(extractFactsFromTurnWithOutcome({
      turnText: TURN, source: 'mcp:put_page', engine: stubEngine({}), embedding: null, sourceContract: contract,
      abortSignal: ac.signal,
    })).rejects.toMatchObject({ name: 'AbortError' });
  });
});
