/**
 * MEM-01 — producer feed pages carry an explicit `gbrain_feed` source
 * contract. Driven through the registered `facts-absorb` minion handler (the
 * real durable path: page snapshot read back from the DB, frontmatter
 * included), with pages stored from producer-shaped markdown via
 * parseMarkdown.
 *
 * Pins the deterministic half of the contract only: source time (not
 * ingestion time), source page context, the dated source label, the subject
 * policy, exact-identity dedup for observations, and a zero-call skip for a
 * present-but-invalid contract. Whether the model phrases a claim correctly
 * ("marked completed" vs "paid") is model quality — not asserted here.
 *
 * Real in-memory PGLite; chat + embedding go through the gateway test
 * transports (no key, no network). All data is synthetic.
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { resetPgliteState } from './helpers/reset-pglite.ts';
import { withEnv } from './helpers/with-env.ts';
import { parseMarkdown } from '../src/core/markdown.ts';
import { buildExtractorSystem } from '../src/core/facts/extract.ts';
import {
  __setChatTransportForTests,
  __setEmbedTransportForTests,
  resetGateway,
  type ChatOpts,
  type ChatResult,
} from '../src/core/ai/gateway.ts';
import { __resetFactsQueueForTests } from '../src/core/facts/queue.ts';
import { _resetWriteThroughCacheForTest } from '../src/core/write-through.ts';
import { MinionWorker } from '../src/core/minions/worker.ts';
import type { MinionJobContext } from '../src/core/minions/types.ts';
import { registerBuiltinHandlers } from '../src/commands/jobs.ts';

let engine: PGLiteEngine;

beforeAll(async () => {
  engine = new PGLiteEngine();
  await engine.connect({});
  await engine.initSchema();
});

afterAll(async () => {
  await engine.disconnect();
});

beforeEach(async () => {
  await resetPgliteState(engine);
});

afterEach(() => {
  __setChatTransportForTests(null);
  __setEmbedTransportForTests(null);
  resetGateway();
  __resetFactsQueueForTests();
});

type StubFact = { fact: string; kind?: string; entity?: string | null; notability?: string };

let chatCalls: ChatOpts[] = [];

function chatStub(facts: StubFact[]): void {
  chatCalls = [];
  __setChatTransportForTests(async (opts: ChatOpts): Promise<ChatResult> => {
    chatCalls.push(opts);
    return {
      text: JSON.stringify({
        facts: facts.map(f => ({
          fact: f.fact,
          kind: f.kind ?? 'fact',
          entity: f.entity ?? null,
          confidence: 1.0,
          notability: f.notability ?? 'high',
        })),
      }),
      blocks: [],
      stopReason: 'end',
      usage: { input_tokens: 0, output_tokens: 0, cache_read_tokens: 0, cache_creation_tokens: 0 },
      model: 'test:stub',
      providerId: 'test',
    };
  });
}

/** Every text embeds to the SAME vector: cosine 1.0 between any two facts. */
function identicalEmbeddings(): string[] {
  const seen: string[] = [];
  __setEmbedTransportForTests((async ({ values }: { values: string[] }) => {
    seen.push(...values);
    return { embeddings: values.map(() => Array.from({ length: 1536 }, () => 0.1)) };
  }) as never);
  return seen;
}

async function storeMarkdown(slug: string, markdown: string): Promise<void> {
  const parsed = parseMarkdown(markdown, `${slug}.md`);
  await engine.putPage(slug, {
    type: parsed.type,
    title: parsed.title,
    compiled_truth: parsed.compiled_truth,
    timeline: parsed.timeline,
    frontmatter: parsed.frontmatter,
  }, { sourceId: 'default' });
}

async function storeEntity(slug: string): Promise<void> {
  await engine.putPage(slug, {
    type: 'person',
    title: slug,
    compiled_truth: `Synthetic entity page ${slug}.`,
    frontmatter: {},
  }, { sourceId: 'default' });
}

async function runAbsorb(slug: string): Promise<Record<string, unknown>> {
  const worker = new MinionWorker(engine, { queue: 'test' });
  await registerBuiltinHandlers(worker, engine, { quiet: true });
  const handler = worker.getHandler('facts-absorb');
  expect(handler).toBeDefined();
  const job: MinionJobContext = {
    id: 1,
    name: 'facts-absorb',
    data: { slug, sourceId: 'default', source: 'mcp:put_page', notabilityFilter: 'all' },
    attempts_made: 0,
    signal: new AbortController().signal,
    deadlineAtMs: null,
    shutdownSignal: new AbortController().signal,
    updateProgress: async () => {},
    updateTokens: async () => {},
    log: async () => {},
    isActive: async () => true,
    readInbox: async () => [],
  };
  return (await handler!(job)) as Record<string, unknown>;
}

type Row = {
  fact: string; entity_slug: string | null; context: string | null;
  valid_from: Date; valid_until: Date | null; expired_at: Date | null; row_num: number | null;
};

async function activeFacts(): Promise<Row[]> {
  return engine.executeRaw<Row>(
    `SELECT fact, entity_slug, context, valid_from, valid_until, expired_at, row_num
       FROM facts WHERE source_id = 'default' AND expired_at IS NULL ORDER BY valid_from, id`,
  );
}

function iso(d: Date): string {
  return new Date(d).toISOString();
}

function feedPage(title: string, feedLines: string[], body: string): string {
  return [
    '---',
    'type: note',
    `title: ${JSON.stringify(title)}`,
    'validate: false',
    'gbrain_feed:',
    ...feedLines.map(l => `  ${l}`),
    '---',
    '',
    `# ${title}`,
    '',
    body,
    '',
  ].join('\n');
}

const COMPLETION_BODY = [
  'Источник: Todoist — журнал закрытых задач за UTC-день 2026-07-23.',
  '',
  '## Закрыто',
  '',
  '- 2026-07-23T09:15:00Z — статус в Todoist: выполнена — текст задачи (автор): «Pay example invoice»',
].join('\n');

function snapshotPage(observedAt: string, count: number, extraFeed: string[] = []): string {
  return feedPage('Открытые задачи (снимок)', [
    'contract: 1', 'producer: "todoist"', 'kind: "task_snapshot"', `observed_at: "${observedAt}"`, ...extraFeed,
  ], [
    `Источник: Todoist — снимок открытых задач на ${observedAt} (UTC).`,
    '',
    `Всего открытых задач: **${count}**.`,
  ].join('\n'));
}

function dailyPage(day: string, steps: number, extraFeed: string[] = []): string {
  return feedPage(`Здоровье — ${day}`, [
    'contract: 1', 'producer: "health"', 'kind: "health_daily"', `measured_on: "${day}"`, ...extraFeed,
  ], [
    `Дата измерений: ${day} (записи устройств за этот день).`,
    '',
    '## Активность',
    '',
    `- Шаги: ${steps}`,
  ].join('\n'));
}

async function withFenceSource<T>(fn: (dir: string) => Promise<T>): Promise<T> {
  const dir = mkdtempSync(join(tmpdir(), 'mem01-fence-'));
  try {
    await engine.executeRaw(`UPDATE sources SET local_path = $1 WHERE id = 'default'`, [dir]);
    await engine.setConfig('sync.write_through', 'true');
    _resetWriteThroughCacheForTest();
    return await fn(dir);
  } finally {
    await engine.executeRaw(`UPDATE sources SET local_path = NULL WHERE id = 'default'`);
    await engine.unsetConfig('sync.write_through');
    _resetWriteThroughCacheForTest();
    rmSync(dir, { recursive: true, force: true });
  }
}

describe('completed-task log (todoist task_completion_log)', () => {
  test('dated to the producer day, labelled as tracker completion status, page context, model entity ignored', async () => {
    await storeEntity('people/alice-example');
    await storeMarkdown('work/tasks/2026-07-23', feedPage('Задачи закрыты — 23 июля 2026', [
      'contract: 1', 'producer: "todoist"', 'kind: "task_completion_log"', 'day: "2026-07-23"',
    ], COMPLETION_BODY));
    chatStub([{ fact: 'Paid example invoice', kind: 'event', entity: 'people/alice-example' }]);

    const result = await runAbsorb('work/tasks/2026-07-23');

    expect(result.inserted).toBe(1);
    const rows = await activeFacts();
    expect(rows).toHaveLength(1);
    expect(rows[0].fact).toBe('[Todoist tracker completion status, UTC day 2026-07-23] Paid example invoice');
    expect(iso(rows[0].valid_from)).toBe('2026-07-23T00:00:00.000Z');
    expect(rows[0].valid_until).toBeNull();
    expect(rows[0].entity_slug).toBeNull();
    expect(rows[0].context).toBe('work/tasks/2026-07-23');

    expect(chatCalls).toHaveLength(1);
    const system = chatCalls[0].system ?? '';
    expect(system.startsWith(buildExtractorSystem(true))).toBe(true);
    expect(system.length).toBeGreaterThan(buildExtractorSystem(true).length);
    const user = String(chatCalls[0].messages[0].content);
    expect(user).toContain('<source_contract>');
    expect(user).toContain('"claim_strength":"recorded_status"');
    expect(user).toContain('"day":"2026-07-23"');
  });
});

// Verbatim output of the producer's merge path (ops scripts/todoist_gbrain.py
// merge_page over a legacy page read back through get_page), and the same page
// after a gbrain round trip, where get_page re-renders the stored contract as
// a JSON flow mapping. Both must reach facts-absorb as the same valid contract.
const MERGED_LEGACY_PAGE = "---\ntype: note\ntitle: \"Задачи закрыты — 23 июля 2026\"\nvalidate: false\ntags: [\"example\"]\ngbrain_feed:\n  contract: 1\n  producer: \"todoist\"\n  kind: \"task_completion_log\"\n  day: \"2026-07-23\"\n---\n\n# Задачи закрыты — 23 июля 2026\n\n## Закрыто\n\n- 09:15 **Pay example invoice** — проект: p1\n\n_Источник: Todoist, время закрытия — UTC._\n\nЗаметка пользователя: сверить с выпиской позже.\n\n## Обновления\n\n### 2026-07-23\n\nИсточник: Todoist — журнал закрытых задач за UTC-день 2026-07-23. «Выполнена» — статус задачи в трекере, а не подтверждение результата; текст задачи — формулировка автора.\n- 2026-07-23T09:15:00Z — статус в Todoist: выполнена — текст задачи (автор): «Pay example invoice» — проект: p1\n";
const ROUND_TRIP_PAGE = "---\ntype: note\ntitle: \"Задачи закрыты — 23 июля 2026\"\nvalidate: false\ngbrain_feed: {\"contract\":1,\"day\":\"2026-07-23\",\"kind\":\"task_completion_log\",\"producer\":\"todoist\"}\ntags: [\"example\"]\n---\n\n\n# Задачи закрыты — 23 июля 2026\n\n## Закрыто\n\n- 09:15 **Pay example invoice** — проект: p1\n\n_Источник: Todoist, время закрытия — UTC._\n\nЗаметка пользователя: сверить с выпиской позже.\n\n## Обновления\n\n### 2026-07-23\n\nИсточник: Todoist — журнал закрытых задач за UTC-день 2026-07-23. «Выполнена» — статус задачи в трекере, а не подтверждение результата; текст задачи — формулировка автора.\n- 2026-07-23T09:15:00Z — статус в Todoist: выполнена — текст задачи (автор): «Pay example invoice» — проект: p1\n";

describe('producer merge output reaches facts-absorb with its contract', () => {
  for (const [name, markdown] of [['block form (merge result)', MERGED_LEGACY_PAGE], ['flow form (gbrain round trip)', ROUND_TRIP_PAGE]] as const) {
    test(name, async () => {
      await storeMarkdown('work/tasks/2026-07-23', markdown);
      const stored = await engine.getPage('work/tasks/2026-07-23', { sourceId: 'default' });
      expect(stored!.compiled_truth).toContain('Заметка пользователя: сверить с выпиской позже.');
      chatStub([{ fact: 'Task "Pay example invoice" marked completed at 09:15:00Z UTC' }]);

      expect((await runAbsorb('work/tasks/2026-07-23')).inserted).toBe(1);
      const rows = await activeFacts();
      expect(rows.map(r => [r.fact, iso(r.valid_from), r.entity_slug, r.context])).toEqual([[
        '[Todoist tracker completion status, UTC day 2026-07-23] Task "Pay example invoice" marked completed at 09:15:00Z UTC',
        '2026-07-23T00:00:00.000Z', null, 'work/tasks/2026-07-23',
      ]]);
      expect(String(chatCalls[0].messages[0].content)).toContain('"kind":"task_completion_log"');
    });
  }
});

describe('open-task snapshots (todoist task_snapshot) on the same UTC day', () => {
  test('DB-only: two snapshots with the same count survive as separate exactly-timed observations', async () => {
    await withEnv({ OPENAI_API_KEY: 'test-key' }, async () => {
      identicalEmbeddings();
      chatStub([{ fact: 'Open tasks: 4', kind: 'fact' }]);
      await storeMarkdown('work/tasks/open', snapshotPage('2026-07-23T08:00:00Z', 4));
      const first = await runAbsorb('work/tasks/open');
      await storeMarkdown('work/tasks/open', snapshotPage('2026-07-23T18:30:00Z', 4));
      const second = await runAbsorb('work/tasks/open');

      expect(first.inserted).toBe(1);
      expect(second.inserted).toBe(1);
      const rows = await activeFacts();
      expect(rows.map(r => r.fact)).toEqual([
        '[Todoist open-task snapshot at 2026-07-23T08:00:00Z] Open tasks: 4',
        '[Todoist open-task snapshot at 2026-07-23T18:30:00Z] Open tasks: 4',
      ]);
      expect(rows.map(r => iso(r.valid_from))).toEqual(['2026-07-23T08:00:00.000Z', '2026-07-23T18:30:00.000Z']);
      for (const r of rows) {
        expect(r.valid_until).toBeNull();
        expect(r.entity_slug).toBeNull();
        expect(r.context).toBe('work/tasks/open');
      }

      // Re-extracting the SAME snapshot (NULL entity bucket) is an exact
      // duplicate of the stored observation, not a third record.
      const replay = await runAbsorb('work/tasks/open');
      expect([replay.inserted, replay.duplicate]).toEqual([0, 1]);
      expect(await activeFacts()).toHaveLength(2);
    });
  });

  test('entity fence: identical embeddings do not collapse same-day snapshots; the full timestamp survives the date-only fence', async () => {
    await withEnv({ OPENAI_API_KEY: 'test-key' }, async () => {
      await withFenceSource(async (dir) => {
        await storeEntity('people/example-owner');
        identicalEmbeddings();
        // The model names the same entity as the contract subject, so the
        // pre-contract pipeline also buckets both facts together and its
        // cosine dedup is what gets exercised.
        chatStub([{ fact: 'Open tasks: 4', kind: 'fact', entity: 'people/example-owner' }]);
        const subject = ['subject: "people/example-owner"'];
        await storeMarkdown('work/tasks/open', snapshotPage('2026-07-23T08:00:00Z', 4, subject));
        const first = await runAbsorb('work/tasks/open');
        await storeMarkdown('work/tasks/open', snapshotPage('2026-07-23T18:30:00Z', 4, subject));
        const second = await runAbsorb('work/tasks/open');

        expect(first.inserted).toBe(1);
        expect(second.inserted).toBe(1);
        expect(second.duplicate).toBe(0);
        const rows = await activeFacts();
        expect(rows.map(r => r.fact)).toEqual([
          '[Todoist open-task snapshot at 2026-07-23T08:00:00Z] Open tasks: 4',
          '[Todoist open-task snapshot at 2026-07-23T18:30:00Z] Open tasks: 4',
        ]);
        for (const r of rows) {
          expect(r.entity_slug).toBe('people/example-owner');
          expect(r.row_num).not.toBeNull();
          // The fence stores valid_from as a DATE: both rows read back as the
          // same day, which is why the label must carry the full timestamp.
          expect(iso(r.valid_from).slice(0, 10)).toBe('2026-07-23');
          expect(r.context).toBe('work/tasks/open');
        }
        const fence = readFileSync(join(dir, 'people/example-owner.md'), 'utf-8');
        expect(fence).toContain('2026-07-23T08:00:00Z');
        expect(fence).toContain('2026-07-23T18:30:00Z');
      });
    });
  });
});

describe('observation dedup is exact identity, never cosine proximity', () => {
  test('different values in the same period both survive; the exact same claim dedups; the same text in another period does not', async () => {
    await withEnv({ OPENAI_API_KEY: 'test-key' }, async () => {
      await storeEntity('people/example-owner');
      identicalEmbeddings();
      const subject = ['subject: "people/example-owner"'];
      // Model entity == contract subject, so the pre-contract pipeline buckets
      // every fact together and its cosine dedup is what gets exercised.
      const owner = 'people/example-owner';

      chatStub([{ fact: 'Steps: 9000', kind: 'fact', entity: owner }]);
      await storeMarkdown('health/2026-08-23', dailyPage('2026-08-23', 9000, subject));
      expect((await runAbsorb('health/2026-08-23')).inserted).toBe(1);

      // Corrected record for the SAME measurement day: different value.
      chatStub([{ fact: 'Steps: 9100', kind: 'fact', entity: owner }]);
      await storeMarkdown('health/2026-08-23', dailyPage('2026-08-23', 9100, subject));
      const corrected = await runAbsorb('health/2026-08-23');
      expect(corrected.inserted).toBe(1);
      expect(corrected.duplicate).toBe(0);

      // Exact same labelled claim again: still a duplicate (dedup not disabled).
      chatStub([{ fact: 'Steps: 9100', kind: 'fact', entity: owner }]);
      const again = await runAbsorb('health/2026-08-23');
      expect(again.inserted).toBe(0);
      expect(again.duplicate).toBe(1);

      // Same value text for a DIFFERENT measurement day: a new observation.
      chatStub([{ fact: 'Steps: 9100', kind: 'fact', entity: owner }]);
      await storeMarkdown('health/2026-08-24', dailyPage('2026-08-24', 9100, subject));
      const nextDay = await runAbsorb('health/2026-08-24');
      expect(nextDay.inserted).toBe(1);

      const rows = await activeFacts();
      expect(rows.map(r => [r.fact, iso(r.valid_from), r.entity_slug, r.context])).toEqual([
        ['[Health device record, measured on 2026-08-23] Steps: 9000', '2026-08-23T00:00:00.000Z', 'people/example-owner', 'health/2026-08-23'],
        ['[Health device record, measured on 2026-08-23] Steps: 9100', '2026-08-23T00:00:00.000Z', 'people/example-owner', 'health/2026-08-23'],
        ['[Health device record, measured on 2026-08-24] Steps: 9100', '2026-08-24T00:00:00.000Z', 'people/example-owner', 'health/2026-08-24'],
      ]);
    });
  });
});

describe('weekly aggregate window', () => {
  test('dated to window_start with the exact published window in the label', async () => {
    await storeMarkdown('health/weekly', feedPage('Здоровье — сводка за неделю', [
      'contract: 1', 'producer: "health"', 'kind: "health_weekly"',
      'window_start: "2026-08-17"', 'window_end: "2026-08-23"',
    ], 'Окно сводки: 2026-08-17 — 2026-08-23 (7 календарных дней, включительно).\n\n- Шаги: среднее 9000.0'));
    chatStub([{ fact: 'Average steps 9000', kind: 'fact', entity: 'people/alice-example' }]);

    expect((await runAbsorb('health/weekly')).inserted).toBe(1);
    const block = String(chatCalls[0].messages[0].content);
    expect(block).toContain('"claim_strength":"aggregate","window_start":"2026-08-17","window_end":"2026-08-23"}');
    expect(block).not.toContain('time_zone');
    const rows = await activeFacts();
    expect(rows).toHaveLength(1);
    expect(rows[0].fact).toBe('[Health device weekly summary, window 2026-08-17..2026-08-23] Average steps 9000');
    expect(iso(rows[0].valid_from)).toBe('2026-08-17T00:00:00.000Z');
    expect(rows[0].entity_slug).toBeNull();
    expect(rows[0].context).toBe('health/weekly');
  });
});

describe('subject comes only from a validated explicit contract plus entity resolution', () => {
  test('explicit subject wins over the model entity and over a null model entity', async () => {
    await storeEntity('people/example-owner');
    await storeEntity('people/alice-example');
    const subject = ['subject: "people/example-owner"'];
    chatStub([
      { fact: 'Steps: 7000', entity: 'people/alice-example' },
      { fact: 'Steps: 7001', entity: null },
    ]);
    await storeMarkdown('health/2026-08-20', dailyPage('2026-08-20', 7000, subject));
    expect((await runAbsorb('health/2026-08-20')).inserted).toBe(2);
    const rows = await activeFacts();
    expect(rows.map(r => r.entity_slug)).toEqual(['people/example-owner', 'people/example-owner']);
  });

  test('an explicit subject with no existing entity page stays unattributed', async () => {
    chatStub([{ fact: 'Steps: 7002', entity: 'people/missing-example' }]);
    await storeMarkdown('health/2026-08-21', dailyPage('2026-08-21', 7002, ['subject: "people/missing-example"']));
    expect((await runAbsorb('health/2026-08-21')).inserted).toBe(1);
    const rows = await activeFacts();
    expect(rows[0].entity_slug).toBeNull();
  });

  test('absent subject: a slug-shaped guess by the model is ignored', async () => {
    await storeEntity('people/example-owner');
    chatStub([{ fact: 'Steps: 7003', entity: 'people/example-owner' }]);
    await storeMarkdown('health/example-owner-2026-08-22', dailyPage('2026-08-22', 7003));
    expect((await runAbsorb('health/example-owner-2026-08-22')).inserted).toBe(1);
    const rows = await activeFacts();
    expect(rows[0].entity_slug).toBeNull();
  });
});

describe('a present but invalid contract skips before any model call', () => {
  const base = ['contract: 1', 'producer: "health"'];
  const cases: Array<[string, string[]]> = [
    ['impossible calendar date', ['contract: 1', 'producer: "todoist"', 'kind: "task_completion_log"', 'day: "2026-02-30"']],
    ['non-leap Feb 29', ['contract: 1', 'producer: "todoist"', 'kind: "task_completion_log"', 'day: "2025-02-29"']],
    ['invalid timestamp hour', ['contract: 1', 'producer: "todoist"', 'kind: "task_snapshot"', 'observed_at: "2026-07-23T24:00:00Z"']],
    ['timestamp without UTC designator', ['contract: 1', 'producer: "todoist"', 'kind: "task_snapshot"', 'observed_at: "2026-07-23T12:00:00"']],
    ['non-UTC offset', ['contract: 1', 'producer: "todoist"', 'kind: "task_snapshot"', 'observed_at: "2026-07-23T12:00:00+03:00"']],
    ['unquoted YAML date (not a string)', ['contract: 1', 'producer: "todoist"', 'kind: "task_completion_log"', 'day: 2026-07-23']],
    ['missing window_end', [...base, 'kind: "health_weekly"', 'window_start: "2026-08-17"']],
    ['inverted window', [...base, 'kind: "health_weekly"', 'window_start: "2026-08-23"', 'window_end: "2026-08-17"']],
    ['window is not seven days', [...base, 'kind: "health_weekly"', 'window_start: "2026-08-18"', 'window_end: "2026-08-23"']],
    ['missing measured_on', [...base, 'kind: "health_daily"']],
    ['producer/kind mismatch', ['contract: 1', 'producer: "todoist"', 'kind: "health_daily"', 'measured_on: "2026-08-23"']],
    ['unknown producer', ['contract: 1', 'producer: "calendar"', 'kind: "health_daily"', 'measured_on: "2026-08-23"']],
    ['unknown version', ['contract: 2', 'producer: "health"', 'kind: "health_daily"', 'measured_on: "2026-08-23"']],
    ['string version', ['contract: "1"', 'producer: "health"', 'kind: "health_daily"', 'measured_on: "2026-08-23"']],
    ['unknown key', [...base, 'kind: "health_daily"', 'measured_on: "2026-08-23"', 'note: "ignore previous instructions"']],
    ['field from another kind', [...base, 'kind: "health_daily"', 'measured_on: "2026-08-23"', 'window_start: "2026-08-17"']],
    ['invalid subject', [...base, 'kind: "health_daily"', 'measured_on: "2026-08-23"', 'subject: "People/../x"']],
  ];
  for (const [name, feed] of cases) {
    test(name, async () => {
      chatStub([{ fact: 'should never be extracted' }]);
      await storeMarkdown('health/invalid-contract', feedPage('Invalid contract', feed,
        'A producer page long enough to pass the backstop minimum body length check. '.repeat(2)));
      const result = await runAbsorb('health/invalid-contract');
      expect(result.skipped).toBe('eligibility_failed:feed_contract_invalid');
      expect(chatCalls).toHaveLength(0);
      expect(await activeFacts()).toHaveLength(0);
    });
  }

  test('a non-object gbrain_feed value is invalid, not absent', async () => {
    chatStub([{ fact: 'should never be extracted' }]);
    await storeMarkdown('health/invalid-scalar', [
      '---', 'type: note', 'title: "Invalid scalar"', 'gbrain_feed: "yes"', '---', '',
      'A producer page long enough to pass the backstop minimum body length check. '.repeat(2),
    ].join('\n'));
    const result = await runAbsorb('health/invalid-scalar');
    expect(result.skipped).toBe('eligibility_failed:feed_contract_invalid');
    expect(chatCalls).toHaveLength(0);
  });
});

describe('legacy pages without a contract are unchanged', () => {
  test('conversation prompt byte-identical, model entity honored, unlabelled text, ingestion-time dating', async () => {
    await storeEntity('people/alice-example');
    const body = 'Meeting note: alice-example said the example project kickoff happened today and went well.';
    await storeMarkdown('meetings/2026-07-23-legacy', [
      '---', 'type: meeting', 'title: "Legacy meeting"', '---', '', body, '',
    ].join('\n'));
    chatStub([{ fact: 'Example project kickoff happened', kind: 'event', entity: 'people/alice-example' }]);

    const before = Date.now();
    expect((await runAbsorb('meetings/2026-07-23-legacy')).inserted).toBe(1);
    const after = Date.now();

    expect(chatCalls).toHaveLength(1);
    expect(chatCalls[0].system).toBe(buildExtractorSystem(true));
    const stored = await engine.getPage('meetings/2026-07-23-legacy', { sourceId: 'default' });
    expect(String(chatCalls[0].messages[0].content))
      .toBe(`<turn>\n${stored!.compiled_truth.trim()}\n</turn>\n\nExtract up to 10 facts.`);
    const rows = await activeFacts();
    expect(rows).toHaveLength(1);
    expect(rows[0].fact).toBe('Example project kickoff happened');
    expect(rows[0].entity_slug).toBe('people/alice-example');
    const t = new Date(rows[0].valid_from).getTime();
    expect(t).toBeGreaterThanOrEqual(before - 1000);
    expect(t).toBeLessThanOrEqual(after + 1000);
  });
});
