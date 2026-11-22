import { describe, expect, test } from 'bun:test';
import { createEvidenceOperation, gbrainHits, runEvidencePack } from '../src/core/ops/evidence-operation.ts';

const ranked = JSON.stringify({
  status: 'ranked',
  calls: 1,
  dropped: 0,
  backends: { gbrain: 'ok', exa: 'empty', tavily: 'ok', parallel: 'skipped' },
  evidence: [{
    backend: 'tavily',
    title: 'Rate',
    url: 'https://example.com/t',
    date: '2026-09-01',
    highlight: '3.75 percent',
    ref: 'https://example.com/t',
    noul: 0.8,
    token: 'secret',
  }],
});

describe('pack_evidence operation', () => {
  test('passes through a ranked pack and drops extra fields', async () => {
    const result = await runEvidencePack(
      { question: 'Bank Rate', sensitive: false },
      [{ slug: 'wiki/rate', chunk_text: '3.75' }],
      async () => ranked,
    );
    expect(result.status).toBe('ranked');
    expect(result.evidence).toEqual([{
      backend: 'tavily',
      title: 'Rate',
      url: 'https://example.com/t',
      date: '2026-09-01',
      highlight: '3.75 percent',
      ref: 'https://example.com/t',
      noul: 0.8,
    }]);
    expect(JSON.stringify(result)).not.toContain('secret');
  });

  test('sensitive and dry-run skip the helper', async () => {
    let called = false;
    const runner = async () => { called = true; return ''; };
    expect((await runEvidencePack({ question: 'Private' }, [], runner)).status).toBe('skipped_sensitive');
    expect((await runEvidencePack({ question: 'Rate', sensitive: false, dry_run: true }, [], runner)).status).toBe('dry_run');
    expect(called).toBe(false);
  });

  test('oversized question does not spawn', async () => {
    let called = false;
    const result = await runEvidencePack(
      { question: 'x'.repeat(2001), sensitive: false },
      [],
      async () => { called = true; return ''; },
    );
    expect(called).toBe(false);
    expect(result.status).toBe('unavailable');
  });

  test('helper failure does not leak stderr', async () => {
    const result = await runEvidencePack(
      { question: 'Rate', sensitive: false },
      [],
      async () => { throw new Error('token secret'); },
    );
    expect(result.status).toBe('unavailable');
    expect(JSON.stringify(result)).not.toContain('secret');
  });

  test('search failure is an error status and still calls the helper', async () => {
    let body = '';
    const op = createEvidenceOperation(
      async () => { throw new Error('db down'); },
      async (input) => {
        body = input;
        return JSON.stringify({
          status: 'empty',
          calls: 0,
          dropped: 0,
          backends: { gbrain: 'error', exa: 'empty', tavily: 'empty', parallel: 'empty' },
          evidence: [],
        });
      },
    );
    const result = await op.handler({ dryRun: false }, { question: 'Rate', sensitive: false });
    expect(result.status).toBe('empty');
    expect(JSON.parse(body).gbrain_status).toBe('error');
    expect(JSON.stringify(result)).not.toContain('db down');
  });

  test('dry-run does not search the brain', async () => {
    let searched = false;
    const op = createEvidenceOperation(async () => { searched = true; return []; });
    const result = await op.handler({ dryRun: true }, { question: 'Rate', sensitive: false });
    expect(searched).toBe(false);
    expect(result.status).toBe('dry_run');
  });

  test('gbrain hit mapping caps text and rejects a non-array', () => {
    expect(gbrainHits(null).status).toBe('error');
    const mapped = gbrainHits([{ slug: 'wiki/rate', title: 'Rate', chunk_text: 'x'.repeat(500), effective_date: '2026-09-01' }]);
    expect(mapped.status).toBe('ok');
    expect((mapped.hits[0] as { chunk_text: string }).chunk_text).toHaveLength(400);
  });
});
