// Lumenix fork: pack_evidence calls the in-process `search` handler with the caller's ctx and hands
// its rows to an external judge. Pins that the 0.60 search shape still yields real hits for a remote caller.
import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { installFixtureChunks } from './helpers/page-projection.ts';
import type { OperationContext } from '../src/core/operations.ts';
import { createEvidenceOperation } from '../src/core/ops/evidence-operation.ts';
import { searchBrain } from '../src/core/ops/lumenix-evidence.ts';

const SRC = 'default';
const BODY = 'EVIDMARK the launch review moved to Thursday afternoon.';
let engine: PGLiteEngine;

const ctx = (remote: boolean) => ({
  engine, config: { engine: 'pglite' }, remote, dryRun: false, sourceId: SRC, transport: 'stdio',
  logger: { info() {}, warn() {}, error() {}, debug() {} },
}) as unknown as OperationContext;

beforeAll(async () => {
  engine = new PGLiteEngine();
  await engine.connect({});
  await engine.initSchema();
  await engine.setConfig('schema_pack', 'gbrain-base');
  await engine.putPage('notes/evidence-example', { type: 'note', title: 'Launch review', compiled_truth: BODY, frontmatter: {} }, { sourceId: SRC });
  await installFixtureChunks(engine, 'notes/evidence-example', [{ chunk_index: 0, chunk_text: BODY, chunk_source: 'compiled_truth', token_count: 10 }], { sourceId: SRC });
}, 120_000);

afterAll(async () => { await engine.disconnect(); }, 60_000);

describe('pack_evidence brain arm', () => {
  for (const remote of [true, false]) {
    test(`remote=${remote}: the judge receives the page as a gbrain hit`, async () => {
      const seen: Array<{ gbrain_status: string; gbrain_hits: unknown[] }> = [];
      const op = createEvidenceOperation((c, q) => searchBrain(c as OperationContext, q), async (input) => {
        seen.push(JSON.parse(input));
        return JSON.stringify({ status: 'empty', evidence: [], calls: 1, backends: { gbrain: 'ok' } });
      });
      const out = await op.handler(ctx(remote), { question: 'EVIDMARK launch review', sensitive: false }) as { status: string };
      expect(out.status).toBe('empty');
      expect(seen).toHaveLength(1);
      expect(seen[0]!.gbrain_status).toBe('ok');
      // 0.60 fences remote rows; the judge must get the unfenced text, as on 0.58.
      expect(seen[0]!.gbrain_hits[0]).toEqual({ slug: 'notes/evidence-example', title: 'Launch review', chunk_text: BODY, effective_date: '' });
    });
  }
});
