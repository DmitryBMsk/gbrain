/**
 * MEM-01 production candidate: canonical Gemini 3.8 Flash pricing with its
 * published 2027-01-01 rate change, and a budget that stays safe across it.
 *
 * Google standard tier (https://ai.google.dev/gemini-api/docs/pricing,
 * verified 2026-09-28): through 2026-12-31 UTC $0.75 in / $3.75 out per 1M
 * tokens; from 2027-01-01T00:00:00Z $1.50 / $7.50. Output includes thinking.
 *
 * Every time-dependent assertion pins its own instant (an explicit `at`, or
 * bun's setSystemTime restored in `finally`), so no verdict here depends on
 * the wall clock or changes on 2027-01-01. New exports are read through
 * namespace imports so a missing symbol fails its own test, not the file.
 */
import { afterEach, describe, expect, setSystemTime, test } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import * as pricing from '../src/core/model-pricing.ts';
import * as budget from '../src/core/budget/budget-tracker.ts';
import { chat, withBudgetTracker, __setChatTransportForTests, type ChatResult } from '../src/core/ai/gateway.ts';
import { estimateChatCostUsd, registerChatUsageSink, type ChatUsageRecord } from '../src/core/ai/chat-usage.ts';

const MODEL = 'google:gemini-3.8-flash';
const INTRO = { input: 0.75, output: 3.75 };
const STANDARD = { input: 1.5, output: 7.5 };
const LAST_INTRO_MS = Date.parse('2026-12-31T23:59:59.999Z');
const CHANGE_MS = Date.parse('2027-01-01T00:00:00.000Z');
const BEFORE = '2026-12-31T23:59:30.000Z';
const AFTER = '2027-01-01T00:00:30.000Z';
/** Digest of the 34 canonical rows that existed before 3.8 was added (captured on the unmodified table). */
const OTHER_ROWS_SHA256 = 'af97b5ba957e6aa49444552eb72b46d2a7c75c65fcb7b842768ec933d9e0a507';

type Rate = { input: number; output: number };
const usd = (p: Rate, input: number, output: number) => (input * p.input + output * p.output) / 1_000_000;
const est = (inputTokens: number, outputTokens: number) =>
  ({ modelId: MODEL, estimatedInputTokens: inputTokens, maxOutputTokens: outputTokens, kind: 'chat' as const });

function atInstant<T>(iso: string, fn: () => T): T {
  setSystemTime(new Date(iso));
  try { return fn(); } finally { setSystemTime(); }
}

const tmpDirs: string[] = [];
function auditPath(): string {
  const dir = mkdtempSync(join(tmpdir(), 'gbrain-mem01-pricing-'));
  tmpDirs.push(dir);
  return join(dir, 'budget.jsonl');
}
function auditLines(path: string): Array<Record<string, unknown>> {
  return readFileSync(path, 'utf8').trim().split('\n').map((l) => JSON.parse(l) as Record<string, unknown>);
}

afterEach(() => {
  setSystemTime();
  __setChatTransportForTests(null);
  for (const d of tmpDirs.splice(0)) rmSync(d, { recursive: true, force: true });
});

describe('canonical dated rate for google:gemini-3.8-flash', () => {
  test('introductory rate through 2026-12-31 UTC, standard rate from 2027-01-01T00:00:00Z', () => {
    expect(pricing.canonicalLookup(MODEL, LAST_INTRO_MS)).toEqual(INTRO);
    expect(pricing.canonicalLookup(MODEL, CHANGE_MS)).toEqual(STANDARD);
    expect(pricing.canonicalLookup(MODEL, new Date(LAST_INTRO_MS))).toEqual(INTRO);
    expect(pricing.canonicalLookup(MODEL, new Date(CHANGE_MS))).toEqual(STANDARD);
  });

  test('without an explicit instant the lookup uses the current time', () => {
    expect(atInstant(BEFORE, () => pricing.canonicalLookup(MODEL))).toEqual(INTRO);
    expect(atInstant(AFTER, () => pricing.canonicalLookup(MODEL))).toEqual(STANDARD);
  });

  test('colon, slash and case-folded ids resolve the dated rate, also after the folded cache is warm', () => {
    // Warm the lazily built case-folded view with an unrelated cased id first.
    expect(pricing.canonicalLookup('ANTHROPIC:claude-opus-4-8')).toMatchObject({ input: 5, output: 25 });
    for (const id of [MODEL, 'google/gemini-3.8-flash', 'Google:Gemini-3.8-Flash', 'GOOGLE/GEMINI-3.8-FLASH', 'google:GEMINI-3.8-flash']) {
      expect(pricing.canonicalLookup(id, LAST_INTRO_MS)).toEqual(INTRO);
      expect(pricing.canonicalLookup(id, CHANGE_MS)).toEqual(STANDARD);
      // A warm cache must not pin whichever rate was resolved first.
      expect(pricing.canonicalLookup(id, LAST_INTRO_MS)).toEqual(INTRO);
    }
  });

  test('unknown and alias ids stay fail-closed on both sides of the change', () => {
    const unknown = [
      'gemini-3.8-flash',                    // bare ids default to anthropic:
      'google:gemini-3.8-flash-latest',
      'google:gemini-3.8-flash-preview',
      'google:gemini-3.8-flash-001',
      'google:gemini-3.8-pro',
      'google:gemini-3.8',
      'openrouter:google/gemini-3.8-flash',  // router markup is never repriced as native
    ];
    for (const id of unknown) {
      expect(pricing.canonicalLookup(id, LAST_INTRO_MS)).toBeUndefined();
      expect(pricing.canonicalLookup(id, CHANGE_MS)).toBeUndefined();
      expect(budget.isModelPriceable(id, 'chat')).toBe(false);
    }
    // Inherited object names are never a pricing key (nor a schedule key).
    for (const id of ['constructor', 'toString', '__proto__', 'Constructor', 'hasOwnProperty']) {
      expect(pricing.canonicalLookup(id, LAST_INTRO_MS)).toBeUndefined();
    }
    const t = new budget.BudgetTracker({ maxCostUsd: 1, label: 'mem01', auditPath: auditPath() });
    let err: unknown;
    try { t.reserve({ ...est(1000, 100), modelId: 'google:gemini-3.8-flash-latest' }); } catch (e) { err = e; }
    expect(err).toBeInstanceOf(budget.BudgetExhausted);
    expect((err as budget.BudgetExhausted).reason).toBe('no_pricing');
  });

  test('every other canonical rate is unchanged and undated', () => {
    const others = Object.keys(pricing.CANONICAL_PRICING).filter((k) => k !== MODEL).sort();
    expect(others).toHaveLength(34);
    const rows = others.map((k) => [k, pricing.CANONICAL_PRICING[k]]);
    expect(createHash('sha256').update(JSON.stringify(rows)).digest('hex')).toBe(OTHER_ROWS_SHA256);
    for (const k of others) {
      expect(pricing.canonicalLookup(k, LAST_INTRO_MS)).toEqual(pricing.CANONICAL_PRICING[k]);
      expect(pricing.canonicalLookup(k, CHANGE_MS)).toEqual(pricing.CANONICAL_PRICING[k]);
    }
  });

  test('the reservation ceiling is the highest rate in effect now or scheduled later', () => {
    const ceiling = (pricing as Record<string, unknown>).canonicalReservationLookup as
      ((id: string, at?: number | Date) => Rate | undefined) | undefined;
    expect(typeof ceiling).toBe('function');
    expect(ceiling!(MODEL, LAST_INTRO_MS)).toEqual(STANDARD);
    expect(ceiling!(MODEL, CHANGE_MS)).toEqual(STANDARD);
    expect(ceiling!('Google/Gemini-3.8-Flash', LAST_INTRO_MS)).toEqual(STANDARD);
    expect(ceiling!('google:gemini-3.8-flash-latest', LAST_INTRO_MS)).toBeUndefined();
    for (const k of Object.keys(pricing.CANONICAL_PRICING).filter((key) => key !== MODEL)) {
      expect(ceiling!(k, LAST_INTRO_MS)).toEqual(pricing.CANONICAL_PRICING[k]);
    }
  });
});

describe('budget tracker across the rate change', () => {
  test('3.8 is priceable, so a capped drain can run it at all', () => {
    expect(budget.isModelPriceable(MODEL, 'chat')).toBe(true);
    expect(budget.isModelPriceable('google/gemini-3.8-flash', 'chat')).toBe(true);
  });

  test('a reservation just before the change refuses a call whose standard-rate settlement would breach the cap', () => {
    // Intro projection $0.01575 would admit under $0.02; the same call settled
    // after midnight costs $0.0315. The ceiling refuses BEFORE any provider call.
    const path = auditPath();
    atInstant(BEFORE, () => {
      const t = new budget.BudgetTracker({ maxCostUsd: 0.02, label: 'mem01', auditPath: path });
      let err: unknown;
      try { t.reserve(est(1000, 4000)); } catch (e) { err = e; }
      expect(err).toBeInstanceOf(budget.BudgetExhausted);
      expect((err as budget.BudgetExhausted).reason).toBe('cost');
      expect(t.totalSpent).toBe(0);
    });
    const denied = auditLines(path).find((l) => l.event === 'reserve_denied')!;
    expect(denied.projected_cost_usd as number).toBeCloseTo(usd(STANDARD, 1000, 4000), 12);
  });

  test('reserve before the change, settle after it: the reservation covers the settlement', () => {
    const path = auditPath();
    const t = atInstant(BEFORE, () => {
      const tracker = new budget.BudgetTracker({ maxCostUsd: 1, label: 'mem01', auditPath: path });
      tracker.reserve(est(1000, 4000));
      return tracker;
    });
    atInstant(AFTER, () => t.record({ modelId: MODEL, inputTokens: 1000, outputTokens: 4000 }));
    const lines = auditLines(path);
    const reserved = lines.find((l) => l.event === 'reserve')!.projected_cost_usd as number;
    const settled = lines.find((l) => l.event === 'record')!.actual_cost_usd as number;
    expect(settled).toBeCloseTo(usd(STANDARD, 1000, 4000), 12);
    expect(reserved).toBeGreaterThanOrEqual(settled);
    expect(t.totalSpent).toBeCloseTo(settled, 12);
  });

  test('actual spend uses the rate published at record time and releases the ceiling reservation', () => {
    const path = auditPath();
    const t = atInstant(BEFORE, () => {
      const tracker = new budget.BudgetTracker({ maxCostUsd: 0.04, label: 'mem01', auditPath: path });
      tracker.reserve(est(1000, 4000));                                          // ceiling $0.0315
      tracker.record({ modelId: MODEL, inputTokens: 1000, outputTokens: 200 });  // intro $0.0015
      // Admitted only if the first ceiling was released: $0.0015 + $0.0315 <= $0.04.
      tracker.reserve(est(1000, 4000));
      return tracker;
    });
    atInstant(AFTER, () => t.record({ modelId: MODEL, inputTokens: 1000, outputTokens: 200 }));  // standard $0.003
    const records = auditLines(path).filter((l) => l.event === 'record').map((l) => l.actual_cost_usd as number);
    expect(records[0]).toBeCloseTo(usd(INTRO, 1000, 200), 12);
    expect(records[1]).toBeCloseTo(usd(STANDARD, 1000, 200), 12);
    expect(t.totalSpent).toBeCloseTo(usd(INTRO, 1000, 200) + usd(STANDARD, 1000, 200), 12);
  });

  test('an operator override still wins over the dated canonical rate (policy unchanged)', () => {
    const path = auditPath();
    const overrides = budget.parsePricingOverrides({ [MODEL]: { input: 2, output: 9 } });
    for (const iso of [BEFORE, AFTER]) {
      atInstant(iso, () => {
        const t = new budget.BudgetTracker({ maxCostUsd: 1, label: 'mem01', auditPath: path, pricingOverrides: overrides });
        t.reserve(est(1000, 100));
        t.record({ modelId: MODEL, inputTokens: 1000, outputTokens: 100 });
        expect(t.totalSpent).toBeCloseTo(usd({ input: 2, output: 9 }, 1000, 100), 12);
      });
    }
    for (const l of auditLines(path).filter((x) => x.event === 'reserve')) {
      expect(l.projected_cost_usd as number).toBeCloseTo(usd({ input: 2, output: 9 }, 1000, 100), 12);
    }
  });

  test('an override that would replace or mask the canonical dated rate is reported as a conflict', () => {
    const conflicts = (budget as Record<string, unknown>).pricingOverrideConflicts as
      ((id: string, o: budget.PricingOverrides | undefined, at?: number) => boolean) | undefined;
    expect(typeof conflicts).toBe('function');
    const o = (rate: Rate, key = MODEL) => budget.parsePricingOverrides({ [key]: rate });
    // Equal to today's rate but masks the published 2027 change (it would pin reservations low).
    expect(conflicts!(MODEL, o(INTRO), LAST_INTRO_MS)).toBe(true);
    // Differs from the rate in effect.
    expect(conflicts!(MODEL, o(STANDARD), LAST_INTRO_MS)).toBe(true);
    expect(conflicts!(MODEL, o({ input: 2, output: 9 }), CHANGE_MS)).toBe(true);
    // Cased/slash spellings are the same model.
    expect(conflicts!('Google/Gemini-3.8-Flash', o(INTRO), LAST_INTRO_MS)).toBe(true);
    // Matches the only rate that will ever apply again: no conflict.
    expect(conflicts!(MODEL, o(STANDARD), CHANGE_MS)).toBe(false);
    // No override, an unrelated override, or an override for a model canonical does not price.
    expect(conflicts!(MODEL, undefined, LAST_INTRO_MS)).toBe(false);
    expect(conflicts!(MODEL, o(INTRO, 'google:gemini-2.5-flash'), LAST_INTRO_MS)).toBe(false);
    expect(conflicts!('litellm:gpt-4o', o({ input: 2.5, output: 10 }, 'litellm:gpt-4o'), LAST_INTRO_MS)).toBe(false);
  });
});

describe('strict override reader for a pinned model', () => {
  type Read = { status: 'ok'; overrides: budget.PricingOverrides | undefined } | { status: 'unreadable' | 'invalid' };
  const reader = () => (budget as Record<string, unknown>).loadPricingOverridesForModel as
    ((engine: { getConfig(key: string): Promise<string | null> }, modelId: string) => Promise<Read>) | undefined;
  const stored = (value: string | null) => ({ getConfig: async (key: string) => (key === 'pricing.overrides' ? value : null) });

  test('unset or blank is a proven absence', async () => {
    expect(typeof reader()).toBe('function');
    for (const v of [null, '', '  ', '{}']) {
      expect(await reader()!(stored(v), MODEL)).toEqual({ status: 'ok', overrides: undefined });
    }
  });

  test('a read failure is unreadable, never absence (the legacy loader stays fail-open)', async () => {
    const failing = { getConfig: async () => { throw new Error('boom'); } };
    expect(await reader()!(failing, MODEL)).toEqual({ status: 'unreadable' });
    expect(await budget.loadPricingOverrides(failing)).toBeUndefined();
  });

  test('an unparseable or non-object document is invalid', async () => {
    for (const v of ['{', 'not json', '[1]', '"x"', '3', 'null', 'true']) {
      expect(await reader()!(stored(v), MODEL)).toEqual({ status: 'invalid' });
    }
  });

  test('a dropped or ambiguous entry for the pinned model is invalid; other models keep lenient parsing', async () => {
    for (const doc of [{ [MODEL]: 'free' }, { [MODEL]: { input: -1 } }, { ' Google:Gemini-3.8-Flash ': null },
      { [MODEL]: 1, 'GOOGLE:gemini-3.8-flash': 1 }]) {
      expect(await reader()!(stored(JSON.stringify(doc)), MODEL)).toEqual({ status: 'invalid' });
    }
    const mixed = { 'google:gemini-2.5-flash': 'free', 'litellm:gpt-4o': { input: 2.5, output: 10 }, [MODEL]: { input: 1.5, output: 7.5 } };
    expect(await reader()!(stored(JSON.stringify(mixed)), MODEL)).toEqual({
      status: 'ok',
      overrides: { 'litellm:gpt-4o': { input: 2.5, output: 10 }, [MODEL]: { input: 1.5, output: 7.5 } },
    });
  });
});

describe('usage accounting prices thinking once, at the rate in effect', () => {
  function stubGemini(): void {
    __setChatTransportForTests(async (): Promise<ChatResult> => ({
      text: '{"facts":[]}', blocks: [], stopReason: 'end',
      // output_tokens already includes thinking (@ai-sdk/google: total = candidates + thoughts).
      usage: { input_tokens: 1200, output_tokens: 3000, cache_read_tokens: 0, cache_creation_tokens: 0 },
      model: MODEL, providerId: 'google',
      providerMetadata: { google: { usageMetadata: { candidatesTokenCount: 500, thoughtsTokenCount: 2500 } } },
    }));
  }

  async function oneCall(iso: string): Promise<{ tracked: number; sink: ChatUsageRecord[] }> {
    setSystemTime(new Date(iso));
    const sink: ChatUsageRecord[] = [];
    const deregister = registerChatUsageSink((r) => { sink.push(r); });
    try {
      stubGemini();
      const t = new budget.BudgetTracker({ maxCostUsd: 1, label: 'mem01', auditPath: auditPath() });
      await withBudgetTracker(t, () => chat({ model: MODEL, maxTokens: 4000, messages: [{ role: 'user', content: 'synthetic' }] }));
      return { tracked: t.totalSpent, sink };
    } finally {
      deregister();
      setSystemTime();
    }
  }

  test('before the change: tracker and usage sink both charge input + total output at the introductory rate', async () => {
    const { tracked, sink } = await oneCall(BEFORE);
    expect(tracked).toBeCloseTo(usd(INTRO, 1200, 3000), 12);
    expect(sink).toHaveLength(1);
    expect(sink[0]!.model).toBe(MODEL);
    expect(sink[0]!.output_tokens).toBe(3000);
    expect(sink[0]!.cost_usd).not.toBeNull();
    expect(sink[0]!.cost_usd!).toBeCloseTo(usd(INTRO, 1200, 3000), 12);
    // Not thinking added a second time on top of the total.
    expect(sink[0]!.cost_usd!).not.toBeCloseTo(usd(INTRO, 1200, 5500), 6);
  });

  test('after the change: the same call is charged at the standard rate', async () => {
    const { tracked, sink } = await oneCall(AFTER);
    expect(tracked).toBeCloseTo(usd(STANDARD, 1200, 3000), 12);
    expect(sink[0]!.cost_usd!).toBeCloseTo(usd(STANDARD, 1200, 3000), 12);
  });

  test('estimateChatCostUsd follows the published rate at call time', () => {
    const u = { input_tokens: 1_000_000, output_tokens: 1_000_000 };
    expect(atInstant(BEFORE, () => estimateChatCostUsd(MODEL, u))).toBeCloseTo(INTRO.input + INTRO.output, 12);
    expect(atInstant(AFTER, () => estimateChatCostUsd(MODEL, u))).toBeCloseTo(STANDARD.input + STANDARD.output, 12);
  });
});
